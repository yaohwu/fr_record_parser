# coding: utf-8
# author: yaoh.wu
#
import csv
import logging
import os
import sys

from parse_data import user_data_file_path, save_to_file

dirk_name = "dirk"

dirk_path = "../data/dirk/history.csv"


def main():
    print("find dirk main")


def find_dirk():
    """
    main
    """
    # 配置日志
    logging.basicConfig(
        format='%(asctime)s %(pathname)s line:%(lineno)d %(message)s ',
        # 将日期格式化成正常模式
        datefmt='%Y-%m-%d %H:%M:%S',
        filename="../log/log.dirk.log",
        level=logging.INFO
    )

    os.remove(dirk_path)

    dirk_history = []

    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(user_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        # 去除标题
        next(f_csv)
        for row in f_csv:
            if row[4] == dirk_name:
                dirk_history.append(row)

    save_to_file(dirk_history, dirk_path)


if __name__ == '__main__':
    main()
    find_dirk()
    sys.exit()
