# coding: utf-8
# author: yaoh.wu
import csv
import logging

# 原始数据路径
file_path = "../data/fr_data.txt"
# 粗略处理过后的石距
parsed_data_file_path = "../data/fr_data_p.csv"
# 日志文件路径
log_file_path = "../log/log.log"


def main():
    # 配置日志
    logging.basicConfig(
        format='%(asctime)s %(pathname)s line:%(lineno)d %(message)s ',
        # 将日期格式化成正常模式
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=log_file_path,
        level=logging.DEBUG
    )
    # 第一步
    # 先粗略地处理一下原始数据,
    # 移除存在乱码的数据和一些无需处理的诸如sql数据,
    # 保留下id,tname,type,param,ip,username,userrole,time,logtime,
    # 存储在csv文件中
    simple_parse_data()


def simple_parse_data():
    # 计数器
    count = 0
    # 临时数据列表
    tmp_data_list = []

    with open(file_path, "rb") as file:
        for line in file:
            # 由于其他数据会出现还行现象,不是以id开头的行是其他数据换行生成的数据，这些数据过滤掉
            if line.decode(encoding="utf-8").startswith("[ sql : ") or line.decode(encoding="utf-8").startswith("\"\t"):
                continue
            # 粗略地处理数据
            data = parse_data(line)
            tmp_data_list.append(data)
            count += 1
            # 分块写入文件,避免占用大量内存
            if count > 1000:
                save_to_file(tmp_data_list)
                tmp_data_list.clear()
                count = 0
    # 最后再处理一下末尾阶段不足1000条的数据
    save_to_file(tmp_data_list)
    tmp_data_list.clear()


def save_to_file(data):
    # 存储到文件系统中
    with open(parsed_data_file_path, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, dialect="excel")
        for item in data:
            try:
                writer.writerow(item)
            except UnicodeEncodeError as err:
                logging.debug("happened: " + err.reason)
                continue


# 粗略处理一下数据,移除一些乱码的信息，只保留id,tname,type,param,ip,username,userrole,time,logtime
def parse_data(line):
    # line 是字节数组,需要通过decode转成字符串
    # 转小写,去除前后空格,去除双引号,只保留id,tname,type,param,ip,username,userrole,time,logtime
    line = line.lower().lstrip().rstrip().decode(encoding="utf-8").replace("\"", "").split("\t", maxsplit=9)[0:9]
    return line


def save_to_database(data):
    # todo 存储到数据库中
    # 存储到数据库中
    return data


if __name__ == '__main__':
    main()
