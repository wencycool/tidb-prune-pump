#!/usr/bin/env python3
# coding=utf-8
import logging
import os
import pathlib
import shutil
import socket
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

deploy_dir = "/tidb-deploy/pump-8250"
pump_config_file = pathlib.Path(deploy_dir) / "conf" / "pump.toml"


# 将科学计数法的字符串转换为整数
def scientific_to_int(scientific_str):
    """
    将科学计数法的字符串转换为整数
    :param scientific_str:科学计数法的字符串
    :return:整数
    """
    from decimal import Decimal, ROUND_DOWN
    num = Decimal(scientific_str)
    return int(num.to_integral_value(rounding=ROUND_DOWN))

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


def check_port_inuse(port):
    """
    查看端口是否被占用
    不能通过该方式判断，因为在执行过程中可能占用了端口导致pump无法启动
    :param port:
    :return:
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("0.0.0.0", port))
        s.close()
        return False
    except Exception as e:
        return True  # 端口被占用


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
def get_pump_do_gcTS():
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
def get_drainer_maxCommitTS():
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
def generate_pump_safe_gcTS(drainer_maxCommitTS, offset=10 * 60):
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
    logging.debug(f"generate pump gcTS: {generate_golang_duration(time_diff_with_offset)}, gcTS_unix_timestamp_check: {gcTS_unix_timestamp_check_format}")
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
    do_gcTS = get_pump_do_gcTS()
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
            gcTS_unix_timestamp_check_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(gcTS_unix_timestamp_check))
            logging.debug(f"do pump gcTS: {do_gcTS_unix_timestamp_format} less than gcTS_unix_timestamp_check: {gcTS_unix_timestamp_check_format}, repeat check...")
        time.sleep(1)
        do_gcTS = get_pump_do_gcTS()
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
    drainer_maxCommitTS = get_drainer_maxCommitTS()
    if drainer_maxCommitTS is None:
        return False
    # 生成pump的gcTS
    logging.info("start to generate pump gcTS")
    gcTS, gcTS_unix_timestamp_check = generate_pump_safe_gcTS(drainer_maxCommitTS)
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


def main():
    if do_pump_gc():
        logging.info("do gc success!")
    else:
        logging.info("do gc failed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
