# coding: utf-8
# author: yaoh.wu
#
import csv
import logging
import re

import sys

# 原始数据合法起始字段正则
data_pattern = r"^\"[0-9]{7}\"\t\""

# 原始数据路径
file_path = "../data/fr_data.txt"

# 测试数据路径
test_path = "../data/test.txt"

# 粗略处理过后的数据
parsed_data_file_path = "../data/fr_data_p.csv"

# 存在ip,username,userrole的数据
user_data_file_path = "../data/fr_data_user.csv"

# 其他可能是由定时调度触发计算的数据
schedule_data_file_path = "../data/fr_data_schedule.csv"

# 日志文件路径
log_file_path = "../log/log.log"

page_size = 10000


def main():
    """
    main
    """
    # 配置日志
    logging.basicConfig(
        format='%(asctime)s %(pathname)s line:%(lineno)d %(message)s ',
        # 将日期格式化成正常模式
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=log_file_path,
        level=logging.INFO
    )

    # 第一步
    # 数据格式"ID"\t"tname"\t"type"\t"param"\t"ip"\t"username"\t"userrole"\t"time"\t"logtime"\t"sql"\t"browser"\t"memory"
    # 其中在sql和param处可能会额外换行，因此先将换行调账一下
    # 先粗略地处理一下原始数据,
    # 移除存在乱码的数据
    # 存储在csv文件中
    simple_parse_data()

    # 第二步
    # 数据格式是由第一步生成的csv数据格式，将存在ip,username,userrole的数据同其他数据分隔开
    divide_parsed_data()

    # 2.1 做一下计数，为特征编码做准备
    count_parsed_user_data()

    # 第三步
    # 类别特征编码，将tname，userrole等类别特征进行编码

    sys.exit()


def simple_parse_data():
    """
    先简单的处理一下数据，
    主要目的是数据清理，移除乱码的数据以及在文本中错误的换行修正
    """
    logging.info("simple parse data begin")

    # 临时数据列表
    tmp_data_list = []
    tmp_line = ""

    with open(file_path, "rb") as file:
        for line in file:
            # 由于其他数据会出现还行现象,不是以id开头的行是其他数据换行生成的数据，这些数据修正一下
            # 修正的思路是读到session id之后不是立即处理改行，而是继续读，追加后面行的内容，知道读到下一个session id
            # 字节数据解析成字符串，并移除换行符
            line = line.decode(encoding="utf-8").replace("\r\n", "\n").replace("\n", "")
            # 粗略地处理数据
            if line.startswith("\"ID\"") is False and re.match(data_pattern, line) is None:
                # 既不是表头也不是以id开头，说明时上一项错误换行的内容，记录该内容并继续读下一行
                tmp_line += line
                continue
            else:
                if tmp_line.strip():
                    data = __parse_data(tmp_line)
                    tmp_data_list.append(data)
                    # 分块写入文件,避免占用大量内存
                    if len(tmp_data_list) > page_size:
                        save_to_file(tmp_data_list, parsed_data_file_path)
                        tmp_data_list.clear()
            # 下一个session id的信息
            tmp_line = line.replace("\r\n", "\n").replace("\n", "")

    # 由于最后一条没有下一个session id 帮助触发数据处理了，因此在结尾处主动处理一下最后一条session id
    if tmp_line.strip():
        data = __parse_data(tmp_line)
        tmp_data_list.append(data)
        # 分块写入文件,避免占用大量内存
        if len(tmp_data_list) > page_size:
            save_to_file(tmp_data_list, parsed_data_file_path)
            tmp_data_list.clear()

    # 最后再处理一下末尾阶段不足1000条的数据
    save_to_file(tmp_data_list, parsed_data_file_path)
    tmp_data_list.clear()
    logging.info("simple parse data end")


def divide_parsed_data():
    """
    以ip,username,userrole 三个字段不为空为标准，将数据划分开
    分为：用户访问数据 和 定时调度使用数据 两部分
    """
    logging.info("divide parsed data begin")
    # 用户数据临时数据列表
    user_tmp_list = []
    # 定时调度数据临时数据列表
    schedule_tmp_list = []

    with open(parsed_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        headers = next(f_csv)
        # 写上表头
        user_tmp_list.append(headers)
        schedule_tmp_list.append(headers)

        for row in f_csv:
            # ip username userrole 三项存在数据
            if row[4] != "" and row[5] != "" and row[6] != "":
                user_tmp_list.append(row)
                if len(user_tmp_list) > page_size:
                    save_to_file(user_tmp_list, user_data_file_path)
                    user_tmp_list.clear()
            else:
                schedule_tmp_list.append(row)
                if len(schedule_tmp_list) > page_size:
                    save_to_file(schedule_tmp_list, schedule_data_file_path)
                    schedule_tmp_list.clear()

    # 最后再处理一下末尾阶段不足1000条的数据
    save_to_file(user_tmp_list, user_data_file_path)
    user_tmp_list.clear()
    save_to_file(schedule_tmp_list, schedule_data_file_path)
    schedule_tmp_list.clear()

    logging.info("divide parsed data end")


def count_parsed_user_data():
    """
    针对用户名称，模板名称，用户角色做一下计数
    """
    # fr_data_user.csv
    # id,tname,type,ip,username,userrole,time,logtime,memory
    # tname
    tpl_list = []
    tpl_tmp = []
    # username
    user_list = []
    user_tmp = []
    # userrole
    user_role_list = []
    user_role_tmp = []
    # count
    count_list = []

    with open(user_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        for row in f_csv:
            tpl_tmp.append(row[1])
            user_tmp.append(row[4])
            user_role_tmp.append(row[5])

    for row in set(tpl_tmp):
        tpl_list.append([row])
    for row in set(user_tmp):
        user_list.append([row])
    for row in set(user_role_tmp):
        user_role_list.append([row])

    save_to_file(tpl_list, "../data/fr_data_tpl_count.csv")
    save_to_file(user_list, "../data/fr_data_user_count.csv")
    save_to_file(user_role_list, "../data/fr_data_userrole_count.csv")

    # count
    count_list.append(["tpl_count", "user_count", "user_role_count"])
    count_list.append([len(tpl_list), len(user_list), len(user_role_list)])
    save_to_file(count_list, "../data/fr_data_count.csv")

    logging.info("count data end")


def save_to_file(data, target_file_path):
    """
    存储到文件系统中
    :param data: 数据列表
    :param target_file_path: 文件路径
    """
    with open(target_file_path, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, dialect="excel")
        for item in data:
            try:
                writer.writerow(item)
            except UnicodeEncodeError as err:
                # 移除乱码的数据
                logging.debug("happened: " + err.reason)
                continue


def __parse_data(line):
    """
    粗略处理一下数据，移除一些乱码的信息
    :param line: 字符串数据
    :return: 修正好的，删除了部分暂时无用数据之后的数据 字符串列表
    """
    # line 已经通过decode转成字符串了
    # 转小写,去除前后空格,去除双引号,拆分
    line = line.lower().strip().lstrip("\"").rstrip("\"").split("\"\t\"")
    # ['id', 'tname', 'type', 'param', 'ip', 'username', 'userrole', 'time', 'logtime', 'sql', 'browser', 'memory']
    # 删除param,sql,browser等信息
    # 部分模板路径以 / 开头
    if line[1].startswith("/"):
        line[1] = line[1].replace("/", "", 1)
    # param
    del line[3]
    # sql
    del line[8]
    # browser
    del line[8]
    return line


def __save_to_database(data):
    """
    保存到数据库中
    :param data: 数据
    :return: 保存成功的数据
    """
    # todo 存储到数据库中
    return data


if __name__ == '__main__':
    main()
