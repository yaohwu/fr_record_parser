# coding: utf-8
# author: yaoh.wu
import csv
import logging

file_path = "E:\\record\\fr_exerecord\\fr_exerecord.txt"
parse_data_file_path = "D:\\record\\fr_exerecord\\fr_exerecord_parsed.csv"


def main():
    # Configure the logging system
    logging.basicConfig(
        filename='../log/parse_data.log',
        level=logging.DEBUG
    )
    logging.info("hello")
    # 计数器
    count = 0
    # 临时数据列表
    tmp_data_list = []

    with open(file_path, "rb") as file:
        for line in file:
            if line.decode(encoding="utf-8").startswith("[ sql : ") or line.decode(encoding="utf-8").startswith("\"\t"):
                continue
            data = parse_data(line)
            tmp_data_list.append(data)
            count += 1
            # 分块写入文件，避免占用大量内存
            if count > 1000:
                save_to_file(tmp_data_list)
                tmp_data_list.clear()
                count = 0
    # 最后再处理一下末尾阶段不足1000条的数据
    save_to_file(tmp_data_list)
    tmp_data_list.clear()


def save_to_file(data):
    with open(parse_data_file_path, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, dialect="excel")
        for item in data:
            try:
                writer.writerow(item)
            except UnicodeEncodeError as err:
                logging.warning("happened: " + err.reason)
                continue


def save_to_database(data):
    logging.info("save to database")


def parse_data(line):
    logging.info("parse each line data")
    # 转小写，去除前后空格,去除双引号
    line = line.lower().lstrip().rstrip().decode(encoding="utf-8").replace("\"", "").split("\t", maxsplit=9)[0:9]
    logging.info(line)
    return line


if __name__ == '__main__':
    main()
