#!/usr/bin/env python3
# coding=utf-8
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
import pathlib
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
import json
import http.client
import atexit

# Description: 通过python脚本实现pump的gc清理
# requirements: python3.6+

# 当pump所在的文件系统使用率超过阈值后需要对pump中的数据进行清理，清理规则为：
# pump中的数据必须已经应用到了drainer，即pump中的数据的gcTS小于drainer的maxCommitTS
# pump部署目录必须为tidb用户，且每一个os上只有一个pump进程，且采用默认部署路径/tidb-deploy/pump-8250
# 清理流程为：
# 1. 从pump的http://127.0.0.1:8250/drainers获取maxCommitTS
# 2. 计算pump的gcTS，比drainer的maxCommitTS小10分钟
# 3. 备份pump.toml并修改pump的gc配置，gc=xxhyym
# 4. 通过sudo sysemctl restart pump-8250重启pump，并探测pump是否重启成功
# 5. 利用http://127.0.0.1:8250/debug/gc/triggers触发gc，等待gc完成（该过程是异步的，需要循环检测gc是否完成）
# 6. 利用http://127.0.0.1:8250/metrics获取binlog_pump_storage_done_gc_ts，判断是gc是否已经完成
# 7. 恢复pump.toml配置文件
# 8. 检查pump是否正常运行，状态（http://127.0.0.1:8250/status）是否正常
# 9. 检查pump是否继续接受binlog日志写入（http://127.0.0.1:8250/binlog/recover?op=status)，如果不正常则重置写入状态
# 说明：
# 整个过程会涉及两次pump的重启，需要控制一个集群中不能同时重启所有的pump，否则会导致写binlog暂停

# 当多个pump同时执行gc时，需要保证串行执行，起码不能同时暂停pump，否则会导致写binlog暂停
# 分布式串行执行逻辑：
# 1. 利用127.0.0.1:10080/status查询所有pump的IP和端口
# 2. 依次连接所有pump节点的IP:53556，查看目标脚本是否正在执行，如果重复轮询判断，直到超时退出
# 3. 如果有任何pump节点在执行，则本启动对53556端口的监听，并等待3秒钟后再次判断是否有其他节点在执行（避免同时执行），如果有则等待，则只保留IP最大的节点执行
# 4. 执行完毕后，释放监听端口

# todo 在运行期间通过添加API的方式抑制altermanager对当前pump的告警，避免在运维过程中出现告警。


deploy_dir = "/tidb-deploy/pump-8250"
pump_config_file = pathlib.Path(deploy_dir) / "conf" / "pump.toml"


def get_pump_ps_info():
    """
    找到pump进程的pid、当前工作目录和cmdline列表
    注意：当前用户必须是tidb，且只有一个pump进程
    :return:
    """
    pumps = []
    base_dir = "/proc"
    for sub_dir in os.listdir(base_dir):
        base_sub_dir = os.path.join(base_dir, sub_dir)
        if os.path.isdir(base_sub_dir) and pathlib.Path(base_sub_dir).owner() == "tidb":
            cmdline_file = os.path.join(base_sub_dir, "cmdline")
            if os.path.exists(cmdline_file):
                with open(cmdline_file, 'rb') as f:
                    cmdline_list = f.read().decode('utf-8').split('\x00')
                    cmdline_str = " ".join(cmdline_list)
                    if "bin/pump" == cmdline_list[0]:
                        logging.debug(f"find pump process: {sub_dir}, cmdline: {cmdline_str}")
                        pumps.append([sub_dir, os.path.realpath(os.path.join(base_sub_dir, "cwd")), cmdline_list])
    return pumps


# 前置检查：
def check_before():
    if not os.path.exists(deploy_dir):
        logging.error(f"{deploy_dir} is not exists")
        return False
    # 如果deploy_dir不是tidb用户，则返回False
    if pathlib.Path(deploy_dir).owner() != "tidb":
        logging.error(f"{deploy_dir} is not tidb user")
        return False
    # pump进程必须存在且只有一个
    pumps = get_pump_ps_info()
    len_pumps = len(pumps)
    if len_pumps != 1:
        logging.error(f"pump process is not only one,num: {len_pumps}")
        return False
    return True


# "maxCommitTS":448576839063961607 ，转换为时间格式化
def ts_to_time(ts):
    """
    将CommitTs转换为unix时间戳
    :param ts: tso时间戳
    :return: unix时间戳
    """
    return (ts >> 18) / 1000


# 获取pump的do gcTC
# 从http://192.168.31.100:8250/metrics获取pump的metrics信息，并解析出binlog_pump_storage_done_gc_ts 的值
def get_pump_do_gc_ts():
    """
    获取pump的do gcTS，这里的do gcTS并不是TSO，而是oracle.ExtractPhysical部分，已经做了>>18处理
    :return: 返回物理时间戳，单位为毫秒
    """
    http_url = "http://127.0.0.1:8250/metrics"
    try:
        with urllib.request.urlopen(http_url) as f:
            for line in f:
                line = line.decode('utf-8')
                if line.startswith("binlog_pump_storage_done_gc_ts"):
                    ts = line.split()[1]
                    # 1.711206711226e+12 转换为整数
                    return int(float(ts))
    except Exception as e:
        logging.error(f"get pump gc_ts failed: {e}")
        return None


# 查看drainer的maxCommitTS
# CommitTs转unix时间戳：(CommitTs >> 18) / 1000
# http://192.168.31.100:8250/drainers 获取json数据并解析出drainer的maxCommitTS
def get_drainer_max_commit_ts():
    http_url = "http://127.0.0.1:8250/drainers"
    try:
        with urllib.request.urlopen(http_url) as f:
            data = f.read().decode('utf-8')
            data = json.loads(data)
            max_commit_ts = int(data[0]["maxCommitTS"])
            max_commit_ts_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_to_time(max_commit_ts)))
            logging.debug(f"get drainer maxCommitTS: {max_commit_ts},format: {max_commit_ts_format}")
            return max_commit_ts
    except Exception as e:
        logging.error(f"get drainer maxCommitTS failed: {e}")
        return None


# 给定一个秒的duration，生成golang的时间字符串表达式，如：1h,10m,1h30m5s等，如果不带任何单位则为天，比如：1 代表1天
def generate_golang_duration(duration):
    """
    给定一个秒的duration，生成一个golang duration字符串,不能超过1天
    :param duration:int类型的秒数
    :return:字符串类型的golang duration
    """
    if duration < 0:
        logging.error(f"generate duration failed: duration is less than 0")
        return None
    if duration < 60:
        return f"{duration}s"
    elif duration < 3600:
        return f"{duration // 60}m{duration % 60}s"
    elif duration < 86400:
        return f"{duration // 3600}h{duration % 3600 // 60}m{duration % 60}s"
    else:
        logging.error(f"generate duration failed: duration is more than 1 day")
        return None


# 生成pump的gcTS
def generate_pump_safe_gc_ts(drainer_maxCommitTS, offset=10 * 60):
    """
    生成pump的gcTS，默认为drainer的maxCommitTS - 10分钟，格式为golagn的时间字符串表达式，如：1h,10m,1h30m5s等，如果不带任何单位则为天，比如：1 代表1天
    :param drainer_maxCommitTS: drainer的maxCommitTS，小于该值的数据可以被清理
    :param offset: 默认比drainer的maxCommitTS小offset秒
    :return: 返回（golang的时间字符串表达式，gcTS的unix时间戳校验值）
    """
    if drainer_maxCommitTS is None:
        return None, None
    # 获取maxCommitTS和当前时间的时间差，单位为秒
    time_diff = int(time.time() - (drainer_maxCommitTS >> 18) / 1000)
    # 这里避免两台机器的时间差，往前推迟offset分钟
    time_diff_with_offset = time_diff + offset
    max_txn_timeout_second = 10 * 60
    # 往前推迟1 分钟作为校验值，另外，在pump做gc时内部也会自动做gc - 10分钟（maxTxnTimeoutSecond），因此实际打印的do_gcTS是比传入的值小10分钟
    gcTS_unix_timestamp_check = time.time() - time_diff_with_offset - max_txn_timeout_second - 60
    gcTS_unix_timestamp_check_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(gcTS_unix_timestamp_check))
    logging.debug(
        f"generate pump gcTS: {generate_golang_duration(time_diff_with_offset)}, gcTS_unix_timestamp_check: {gcTS_unix_timestamp_check_format}")
    return generate_golang_duration(time_diff_with_offset), gcTS_unix_timestamp_check


# 修改pump.toml文件，如果存在gc=xxh的配置，则修改为gc=yyh，否则添加gc=yyh配置
# gc = 1
# gc = "3h"
def modify_pump_toml_gc(gc_ts):
    """
    获取pump.toml修改前的文件，方便恢复
    :param gc_ts: golang的时间字符串表达式
    :return: 成功修改则返回备份文件路径，否则返回None
    """
    if not gc_ts:
        logging.error(f"generate pump gcTS failed")
        return None
    # 对pump_toml备份，避免修改失败, 权限一致
    backup_pump_config_file = f"{pump_config_file}.bak"
    try:
        shutil.copyfile(pump_config_file, backup_pump_config_file)
    except Exception as e:
        logging.error(f"backup pump.toml failed: {e}")
        return None
    with open(pump_config_file, 'r') as f:
        lines = f.readlines()
    with open(pump_config_file, 'w') as f:
        modify_gc = False
        for line in lines:
            # 除去所有空格，包括字符之间的空格
            line_bak = line.replace(" ", "")
            if line_bak.startswith("gc="):
                f.write(f'gc = "{gc_ts}"\n')
                modify_gc = True
            else:
                f.write(line)
        if not modify_gc:
            f.write(f'gc = "{gc_ts}"\n')
    return backup_pump_config_file


# 检查pump状态是否正常
def check_pump_status(timeout=30):
    """
    检查pump的状态是否正常
    :param timeout: 超时时间为30s
    :return: True表示正常，False表示异常
    """
    url = "http://127.0.0.1:8250/metrics"

    # 检查url是否可以访问
    def check_url(url1):
        try:
            with urllib.request.urlopen(url1) as f:
                pass
            return True
        except Exception as e:
            return False

    for _ in range(timeout):
        logging.debug(f"check pump status, url: {url}")
        if check_url(url):
            break
        time.sleep(1)
    else:
        logging.error(f"check pump status failed, timeout: {timeout}s")
        return False
    return True


# 重启pump进程
def start_pump(restart=False):
    """
    直接sudo systemctl restart pump-8250.service是异步的，无法判断是否重启成功，需要结合pump的http接口来判断pump是否重启成功
    :return:
    """
    if restart:
        shell_cmd = "sudo systemctl restart pump-8250.service"
    else:
        shell_cmd = "sudo systemctl start pump-8250.service"
    logging.info(f"start or restart pump,shell command: {shell_cmd}")
    recode = os.system(shell_cmd)
    if recode != 0:
        logging.error(f"start or restart pump failed,shell command : {shell_cmd}")
        return False
    # 检查pump是否启动成功
    return check_pump_status()


# 利用trigger立即触发gc动作
def do_pump_gc_trigger(gcTS_unix_timestamp_check: int):
    """
    触发pump的gc动作
    :param gcTS_unix_timestamp_check: gcTS的unix时间戳校验值，如果gcTS 大于该值则表示gc成功
    :return: True表示触发成功，False表示触发失败
    """
    host = "127.0.0.1"
    port = 8250
    try:
        # 做post请求
        logging.debug(f"trigger pump gc, host: {host}, port: {port}")
        conn = http.client.HTTPConnection(host, port, timeout=10)
        conn.request("POST", "/debug/gc/trigger", None, {
            "Content-Type": "application/json"
        })
        response = conn.getresponse()
        data = response.read()
        result = data.decode("utf-8")
        conn.close()
    except Exception as e:
        logging.error(f"trigger pump gc failed: {e}")
        return False
    if "trigger gc success" not in result:
        return False
    # 该接口是异步的，需要循环检测gc是否完成
    logging.debug(f"trigger pump gc success, wait for gc done")
    do_gcTS = get_pump_do_gc_ts()
    if do_gcTS is None:
        return False
    logging.debug(f"do pump gcTS: {do_gcTS}")
    # 等待gc完成，2分钟超时
    timeout = 120
    for _ in range(timeout):
        # 这里的do_gcTS并不是TSO，而是oracle.ExtractPhysical部分，已经做了>>18处理
        do_gcTS_unix_timestamp = do_gcTS / 1000
        if do_gcTS_unix_timestamp > gcTS_unix_timestamp_check:
            return True
        else:
            # do_gcTS_unix_timestamp转为标准时间格式
            do_gcTS_unix_timestamp_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(do_gcTS_unix_timestamp))
            gcTS_unix_timestamp_check_format = time.strftime("%Y-%m-%d %H:%M:%S",
                                                             time.localtime(gcTS_unix_timestamp_check))
            logging.debug(
                f"do pump gcTS: {do_gcTS_unix_timestamp_format} less than gcTS_unix_timestamp_check: {gcTS_unix_timestamp_check_format}, repeat check...")
        time.sleep(1)
        do_gcTS = get_pump_do_gc_ts()
        if do_gcTS is None:
            return False
    else:
        logging.error(f"do pump gc timeout: {timeout}s")
        return False


# [tidb@a-cszx-db01 temp]$ curl http://127.0.0.1:10080/binlog/recover?op=status
# {
#  "Skipped": false,
#  "SkippedCommitterCounter": 0
# }
def tidb_write_pump_skipped(ip="127.0.0.1", port=10080, do=True):
    """
    重置pump的写入状态
    :param ip: 需要查询的tidb-server的ip
    :param port: 需要查询的tidb-server的状态端口
    :param do: True表示执行，False表示不执行，仅查询Skiipped的状态，如果设置为True则会重置Skipped的状态并返回重置后的状态结果
    :return: True表示Skipped为False，False表示Skipped为True
    """
    # todo 只能判断10080端口的tidb-server的binlog写入状态，如果有多个tidb-server则需要修改，其实如果能保证同一个时刻不会暂停所有pump的进程，可以不用重置
    # 查询pump的写入状态
    url = f"http://{ip}:{port}/binlog/recover?op=status"
    try:
        with urllib.request.urlopen(url) as f:
            data = f.read().decode('utf-8')
            data = json.loads(data)
            skipped = data["Skipped"]
    except Exception as e:
        logging.error(f"get pump write status failed: {e}")
        return True
    if not do:
        return skipped
    elif not skipped:
        return False
    if skipped and do:
        # 重置pump的写入状态
        url = f"http://{ip}:{port}/binlog/recover"
        try:
            with urllib.request.urlopen(url) as f:
                data = f.read().decode('utf-8')
                data = json.loads(data)
                skipped = data["Skipped"]
                return skipped
        except Exception as e:
            logging.error(f"reset pump write status failed: {e}")
            return True


# 分布式串行执行逻辑代码
# 获取当前所有pump的IP和端口
# http://192.168.31.101:8250/status
# {"status":{"192.168.31.101:8250":{"nodeId":"192.168.31.101:8250","host":"192.168.31.101:8250","state":"online","isAlive":false,"score":0,"label":null,"maxCommitTS":448599350057893889,"updateTS":448599350254764042},"192.168.31.102:8250":{"nodeId":"192.168.31.102:8250","host":"192.168.31.102:8250","state":"online","isAlive":false,"score":0,"label":null,"maxCommitTS":448599350477586435,"updateTS":448599350372204545}},"CommitTS":448599350922969089,"Checkpoint":{},"ErrMsg":""}
def get_pumps():
    """
    获取所有pump的IP和端口
    :return: 返回所有pump的host(ip:port)列表
    """
    http_url = "http://127.0.0.1:8250/status"
    pumps = []
    try:
        with urllib.request.urlopen(http_url) as f:
            data = f.read().decode('utf-8')
            data = json.loads(data)
            for v in data["status"].values():
                pumps.append(v["host"])
    except Exception as e:
        logging.error(f"get pumps failed: {e}")
        return None
    # pump组成为："ip:port"
    return pumps


# 通过tcp探测远程端口服务是否正常（类似于telnet是否通）
def telnet_remote_port(host, port, timeout=2):
    """
    检查远程端口是否正常
    :param host: 主机ip
    :param port: 端口
    :param timeout: 超时时间
    :return: True表示正常，False表示异常
    """
    import telnetlib
    try:
        tn = telnetlib.Telnet(host, port, timeout)
        tn.close()
        return True
    except Exception as e:
        return False


# 在一批主机和端口中并发探测
def muti_telnet_remote_port(iports, timeout=2):
    """
    并发telnet远程端口号，并返回端口号是否可访问
    :param iports: [(ip,port),...]
    :param timeout:
    :return: [(ip,port,status),...] status为True代表可连通，False代表不通
    """
    results = []

    def _telnet_remote_port(host, port):
        return host, port, telnet_remote_port(host, port, timeout)
    with ThreadPoolExecutor(max_workers=5) as exector:
        tasks = [exector.submit(_telnet_remote_port, ip, port) for ip, port in iports]
        for task in as_completed(tasks):
            results.append(task.result())
    return results


# 查询本地IP
def get_local_ips(ignore_loopback=False):
    """
    查询本地所有IP列表
    :param loopback: 是否包含回环地址
    :return: IP列表
    """
    ip_addresses = []
    # 执行ip a命令获取网络接口信息
    result = subprocess.run(['ip', 'a'], capture_output=True, text=True)
    output = result.stdout
    # 使用正则表达式匹配IP地址信息
    ip_pattern = r'\d+\.\d+\.\d+\.\d+/\d+'
    matches = re.findall(ip_pattern, output)
    # 将匹配到的IP地址添加到列表中
    ip_addresses.extend(matches)
    result = []
    for each_addr in ip_addresses:
        if "/" in each_addr:
            addr = each_addr.split("/")[0]
            if ignore_loopback and addr.startswith("127.0.0.1"):
                continue
            result.append(addr)
    return result


class Lock:
    """
    利用端口号探测模拟分布式锁
    """

    def __init__(self):
        self.port = 53556
        self.locked = False
        self.service = None

    def _is_locked(self, include=False):
        """
        查询锁是否存在
        :param include:是否包含本地已上锁
        :return: 如果已经存在锁则返回True，否则返回False
        """
        pumps = get_pumps()
        if not pumps:
            raise Exception("cannot find pumps")
        iports = [(iport.split(":")[0], self.port) for iport in pumps]
        result = muti_telnet_remote_port(iports)
        for each_result in result:
            host, port, telnet_ok = each_result
            if not include and host in get_local_ips():
                continue
            if telnet_ok:
                logging.debug(f"host:{host},port:{port} telnet ok!")
                return True
        return False

    def acquire(self, timeout=120):
        """
        :param timeout:
        :return: 上锁成功返回True，否则返回False
        """
        start_time = time.time()
        logging.debug(f"start acquire lock")
        while True:
            if time.time() - start_time > timeout:
                logging.error("lock timeout")
                return False
            if not self._is_locked():
                # 开启本地服务，并再次探测其它节点是否存在连通情况
                try:
                    self.service = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.service.bind(("0.0.0.0", self.port))
                    self.service.listen()
                except Exception as e:
                    logging.error(f"bind 0.0.0.0:{self.port} failed,port exists!")
                    return False
                time.sleep(1)
                if self._is_locked():
                    self.service.close()
                    continue
                else:
                    self.locked = True
                    atexit.register(lambda: self.release())
                    return True
            time.sleep(1)

    def release(self):
        if self.service:
            try:
                self.service.close()
            except Exception as e:
                pass


# todo 添加文件系统使用率判断规则


# 执行清理pump逻辑
def do_pump_gc():
    """
    执行pump的gc清理逻辑
    :return: True表示清理成功，False表示清理失败
    """
    # 前置检查
    if not check_before():
        return False
    # 获取drainer的maxCommitTS
    logging.info("start to get drainer maxCommitTS")
    drainer_maxCommitTS = get_drainer_max_commit_ts()
    if drainer_maxCommitTS is None:
        return False
    # 生成pump的gcTS
    logging.info("start to generate pump gcTS")
    gcTS, gcTS_unix_timestamp_check = generate_pump_safe_gc_ts(drainer_maxCommitTS)
    if gcTS is None:
        return False
    # 修改pump.toml文件
    logging.info("start to modify pump.toml")
    backup_pump_config_file = modify_pump_toml_gc(gcTS)
    if backup_pump_config_file is None:
        return False
    else:
        # 恢复pump.toml文件
        logging.info("start to recover pump.toml")
        atexit.register(lambda: shutil.copyfile(backup_pump_config_file, pump_config_file))
    # 重启pump
    logging.info("start to restart pump")
    if not start_pump(restart=True):
        return False
    # 触发gc
    logging.info("start to trigger pump gc")
    if not do_pump_gc_trigger(gcTS_unix_timestamp_check):
        return False
    # 重启pump
    logging.info("start to restart pump")
    if not start_pump(restart=True):
        return False
    # 检查pump状态
    logging.info("start to check pump status")
    if not check_pump_status():
        return False
    # logging.info("start to check tidb write pump status")
    skipped = tidb_write_pump_skipped(do=True)
    if skipped:
        logging.error(f"tidb write pump status is skipped")
        return False
    return True


# 文件锁，方式程序重复执行
class ProcessLock:
    """
    制定锁注册机制，避免一个脚本重复多次执行，给定一个锁文件，如果锁文件存在则判定为脚本已经在执行，否则判定为脚本未执行
    机制为：
    在程序获取锁之前：
    1、如果锁文件不存在，则创建锁文件，获锁成功
    2、如果锁文件存在，且锁文件中的pid在当前进程列表中不存在，则判定为残留锁文件，清理之，获锁成功
    3、如果锁文件存在，且锁文件中的pid在当前进程列表中存在，但是mtime超过20小时未更新，则判定为残留锁文件，清理之，获锁成功
    4、其余情况判定为锁文件被当前进程占用，获取锁失败
    在程序获取锁之后：
    1、定期更新锁文件的mtime为当前时间
    2、如果文件不存在或文件中pid不是当前进程id，则抢锁（__acquire_attempts），并维护锁文件
    3、抢锁次数不超过max_acquire_attempts次,超过后抛出异常
    使用方式：
    with ProcessLock(os.path.join("/tmp", proj_name + ".lock")) as lock:
        if lock.locked:
            do something
        else:
            logging.info("The script is running,exit!")
    """

    def __init__(self, lock_file):
        self.lock_file = lock_file
        self.locked = False
        # 避免极端情况下篡改锁文件内容导致多进程相互抢锁，这里限制抢锁次数不得超过10次
        self.max_acquire_attempts = 10
        # keepalive的探测周期,默认60秒
        self.keep_alive_interval = 60
        self.__acquire_attempts = 0
        self.__keep_alived = False
        self.atexit()

    # 当进程退出时，清理锁文件
    def atexit(self):
        atexit.register(self.release)

    def acquire(self):
        if self.__acquire():
            self.locked = True
            self.keep_alive()
            return True
        return False

    # 尝试获取锁，如果获取成功则返回True，否则返回False
    def __acquire(self):
        if self.locked:
            return True
        elif os.path.isfile(self.lock_file):
            # 1、如果锁文件存在，且内容是当前进程的pid，则判断为锁文件被当前进程占用，返回True
            if str(os.getpid()) == open(self.lock_file).read():
                # 虽然pid在操作系统当前进程中存在但是不一定是“当前脚本的”，因此结合mtime进一步判断是否脚本的mtime有定期更新，没有定期更新说进程确实不存在
                # 为避免误操作，这里谨慎处理，如果文件未变化超过20小时则说明确实不存在,清理文件锁
                if time.time() - os.path.getmtime(self.lock_file) > 20 * 3600:
                    os.remove(self.lock_file)
                    return self.acquire()
                return True
            # 2、如果文件锁存在，且文件中pid在当前进程列表中不存在，则判定为残留锁文件，清理之
            try:
                infile_pid = int(open(self.lock_file).read())
                if not self.process_id_exists(infile_pid):
                    os.remove(self.lock_file)
                    return self.acquire()
                else:
                    return False
            except ValueError:
                # 说明锁文件中的pid不是数字，清理之
                os.remove(self.lock_file)
                return self.acquire()
        else:
            # 如果锁文件不存在，则创建锁文件
            with open(self.lock_file, "w") as f:
                # 写入当前进程号的pid
                f.write(str(os.getpid()))
            return True

    def release(self):
        if self.locked:
            os.remove(self.lock_file)
            self.locked = False

    # 启动一个线程在后台不停的修改self.lock_file的mtime，防止别人篡改锁文件
    def keep_alive(self):
        if not self.__keep_alived:
            t = threading.Thread(target=self.__keep_alive)
            t.setDaemon(True)
            t.start()
            self.__keep_alived = True

    def __keep_alive(self):
        """
        每分钟检测一次锁文件是否存在，不存在创建之，存在pid不一致则更新之
        """
        while self.locked:
            if self.max_acquire_attempts < self.__acquire_attempts:
                self.release()
                raise Exception("获取锁失败，尝试次数超过%d次" % self.max_acquire_attempts)
            if os.path.isfile(self.lock_file):
                # 如果当前进程号不在锁文件中，更新锁文件
                if str(os.getpid()) != open(self.lock_file).read():
                    with open(self.lock_file, "w") as f:
                        # 写入当前进程号的pid
                        f.write(str(os.getpid()))
                    self.__acquire_attempts += 1
                # 定期更新锁文件的mtime为当前时间
                else:
                    os.utime(self.lock_file, None)
            else:
                with open(self.lock_file, "w") as f:
                    # 写入当前进程号的pid
                    f.write(str(os.getpid()))
                self.__acquire_attempts += 1
            time.sleep(self.keep_alive_interval)

    # 检查进程号是否存在，如果不存在则返回False，否则返回True
    def process_id_exists(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    # 支持with语法
    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


# 查看一个文件（或目录）所在挂载点使用情况
def get_mount_point_usage(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return shutil.disk_usage(path)


# 获取当前目录所在文件系统的使用率
def get_mount_point_used_ratio(path, pre_delete_size=0):
    """
    获取当前目录所在文件系统的使用率
    :param path: 目标目录
    :param pre_delete_size: 预先删除文件大小，在计算使用率时需要减去这个大小
    :return: 返回使用率，如0.6表示当前挂载点使用率低于60%
    """
    mount_point_usage = get_mount_point_usage(path)
    mount_point_used_ratio = round(
        (mount_point_usage.total - mount_point_usage.free - pre_delete_size) / mount_point_usage.total, 2
    )
    return mount_point_used_ratio


def check_do_gc(alarm_ratio=0.7):
    # 判断当前文件系统使用率是否超过70%，如果超过则执行清理逻辑
    pump_mount_point = deploy_dir
    used_ratio = get_mount_point_used_ratio(pump_mount_point)
    if used_ratio < alarm_ratio:
        logging.info(f"{pump_mount_point} usage ratio:{used_ratio} is less than 70%,exit!")
        return
    else:
        logging.warning(f"{pump_mount_point} usage ratio:{used_ratio} is more than 70%,start to do gc!")
    lock = Lock()
    try:
        if lock.acquire(timeout=300):
            logging.info("acquire lock ok")
            if do_pump_gc():
                logging.info("do gc success!")
            else:
                logging.info("do gc failed")
            lock.release()
        else:
            logging.info("acquire lock failed")
    except Exception as e:
        logging.error(f"main error: {e}")
        lock.release()


def main():
    parser = argparse.ArgumentParser(description="tidb-pump清理工具")
    parser.add_argument('a', '--alarm', type=float, default=0.7, help="文件系统使用率告警阈值,超过该阈值则执行gc动作",
                        required=True)
    args = parser.parse_args()
    alarm = args.alarm
    if args.alarm < 0 or args.alarm > 1 or not args.alarm:
        alarm = 0.7
    # 输出到日志文件中，日志文件名为当前脚本的名字加.log后缀
    proj_name = os.path.basename(__file__).split(".")[0]
    # 绝对路径
    file_path = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(file_path, proj_name + ".log")
    # 时间格式为：2017-08-01 10:00:00 main.py[line:10] INFO Start to run the script!
    logging.basicConfig(level=logging.DEBUG,
                        filename=log_file,
                        filemode='a',
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    logging.info("Start to run the script!")
    with ProcessLock(os.path.join("/tmp", proj_name + ".lock")) as lock:
        if lock.locked:
            check_do_gc(alarm)
        else:
            logging.info("The script is running,exit!")


if __name__ == "__main__":
    main()
