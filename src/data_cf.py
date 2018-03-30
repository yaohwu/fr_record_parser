# coding: utf-8
# author: yaoh.wu
#
import csv
import os
import sys
import time

from parse_data import user_count_file_path, save_to_file, tpl_count_file_path, user_data_file_path

# 用户名 id 对应文件
user_id_file_path = "../data/id/username.csv"

# 模板名称 id 对应文件

tpl_id_file_path = "../data/id/tpl.csv"


def main():
    pass


def allocate_id():
    """
    为用户名和模板名绑定id
    :return:用户名id对应关系，模板名id对应关系
    """
    username_id_list = []
    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(user_count_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        user_id = 0
        for row in f_csv:
            username_id_list.append([row[0], user_id])
            user_id += 1

    save_to_file(username_id_list, user_id_file_path)

    tpl_id_list = []
    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(tpl_count_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        tpl_id = 0
        for row in f_csv:
            tpl_id_list.append([row[0], tpl_id])
            tpl_id += 1

    save_to_file(tpl_id_list, tpl_id_file_path)
    return username_id_list, tpl_id_list


def kps(users, tpls, now_time):
    """
    计算兴趣程度
    :param users:用户名id对应关系
    :param tpls:模板名id对应关系
    :param now_time:计算兴趣程度的时间
    """

    now_time = time.mktime(time.strptime(now_time, "%Y/%m/%d %H:%M:%S"))

    over10days = now_time - 10 * 24 * 60 * 60
    over8days = now_time - 8 * 24 * 60 * 60
    over6days = now_time - 6 * 24 * 60 * 60
    over4days = now_time - 4 * 24 * 60 * 60
    over2days = now_time - 2 * 24 * 60 * 60
    # 所有评分
    all_scores = []
    # 模板的评分 key: tname value: user_ksp
    tpls_kps = {}

    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(user_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        # 跳过标配
        next(f_csv)
        for row in f_csv:
            logtime = time.mktime(time.strptime(row[7], "%Y/%m/%d %H:%M:%S"))
            if logtime < over10days:
                score = 0
            elif over8days > logtime > over10days:
                score = 1
            elif over6days > logtime > over8days:
                score = 2
            elif over4days > logtime > over6days:
                score = 3
            elif over2days > logtime > over4days:
                score = 4
            elif now_time > logtime > over2days:
                score = 5
            else:
                score = 0

            if row[1] in tpls_kps:
                tpls_kps[row[1]][row[4]] = score
            else:
                tpls_kps[row[1]] = {}
                tpls_kps[row[1]][row[4]] = score

    for tpl in tpl_list:
        tpl_kps = tpls_kps[tpl[0]]
        tpl_score = []
        for user in user_list:
            if user[0] in tpl_kps:
                tpl_score.append(tpl_kps[user[0]])
            else:
                tpl_score.append(0)
        all_scores.append(tpl_score)
    return all_scores


if __name__ == '__main__':
    main()
    os.remove(user_id_file_path)
    os.remove(tpl_id_file_path)
    user_list, tpl_list = allocate_id()

    result = kps(user_list, tpl_list, "2017/05/04 00:00:00")

    sys.exit()
