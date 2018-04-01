# encoding: utf-8
# author: yaoh.wu

import csv
import os
import sys
import time

import numpy as np
from scipy.stats import pearsonr

from parse_data import user_count_file_path, save_to_file, tpl_count_file_path, user_data_file_path

# 用户名 id 对应文件
user_id_file_path = "../data/id/username.csv"

# 模板名称 id 对应文件

tpl_id_file_path = "../data/id/tpl.csv"

# 评分结果文件 列 为用户，行 为模板 793（用户）*1262（模板）
result_path = "../data/id/result.csv"


def main():
    pass


def get_top_k_near_user(user_id, k):
    users_p = []
    tran_result = np.transpose(result)
    for i in range(len(tran_result)):
        # 只算有评过分的, 不然全是0，不准确
        user_a = result[user_id]
        user_b = result[i]

        user_x = []
        user_y = []

        for inner in range(len(user_a)):
            if user_a[inner] != 0 or user_b[inner] != 0:
                user_x.append(user_a[inner])
                user_y.append(user_b[inner])

        if len(user_y) == 0:
            # 值 推算不出来相似度，权当作完全不相似
            users_p.append((i, -1))
        else:
            p, p_value = pearsonr(user_x, user_y)
            users_p.append((i, p))

    result_user = sorted(users_p, key=lambda user_p: user_p[1], reverse=True)
    print(result_user)


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


def kps(now_time):
    """
    计算兴趣程度
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
            # 存储分数
            if row[1] in tpls_kps:
                if row[4] in tpls_kps[row[1]]:
                    pre = tpls_kps[row[1]][row[4]]
                    tpls_kps[row[1]][row[4]] = max(score, pre)
                else:
                    tpls_kps[row[1]][row[4]] = score
            else:
                tpls_kps[row[1]] = {}
                tpls_kps[row[1]][row[4]] = score

    for tpl in tpl_list:
        tpl_score = []
        # 模板是否有被预览过
        if tpl[0] in tpls_kps.keys():
            tpl_kps = tpls_kps[tpl[0]]
            for user in user_list:
                # 用户是否预览过该模板
                if user[0] in tpl_kps.keys():
                    tpl_score.append(tpl_kps[user[0]])
                else:
                    tpl_score.append(0)
        else:
            tpl_score = [0] * len(user_list)

        all_scores.append(tpl_score)
    return all_scores


def read_result():
    result_from_file = []
    with open(result_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        for row in f_csv:
            result_from_file.append(row)
    return result_from_file


if __name__ == '__main__':
    main()

    if os.path.exists(user_id_file_path):
        os.remove(user_id_file_path)
    if os.path.exists(tpl_id_file_path):
        os.remove(tpl_id_file_path)
    user_list, tpl_list = allocate_id()

    result = kps("2017/05/18 00:00:00")
    if os.path.exists(result_path):
        os.remove(result_path)
    save_to_file(result, result_path)

    # kps 耗时太长了，直接从文件读上次的结果
    # result = read_result()
    get_top_k_near_user(77, 10)

    sys.exit(0)
