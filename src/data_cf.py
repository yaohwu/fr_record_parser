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
        user_a = tran_result[user_id]
        user_b = tran_result[i]

        user_x = []
        user_y = []

        for inner in range(len(user_a)):
            a = int(user_a[inner])
            b = int(user_b[inner])

            user_x.append(a)
            user_y.append(b)
        user_x = np.asarray(user_x)
        user_y = np.asarray(user_y)

        if user_x.mean() == 0 or user_y.mean() == 0:
            # 值 推算不出来相似度，权当作完全不相似
            users_p.append((i, -1))
        else:
            p, p_value = pearsonr(user_x, user_y)
            users_p.append((i, p))
    # 拿掉相关系数为1的用户，因为是自身
    result_user = sorted(users_p, key=lambda user_p: user_p[1], reverse=True)[1:(k + 1)]
    print(result_user)
    return result_user


def get_top_k_tpl_(result_user, k):
    result_tpl_all = []
    tran_result = np.transpose(result)

    for i in range(len(result_user)):
        user_id = result_user[i][0]
        user_p = result_user[i][1]

        tem_list = []
        user_tpl_score = tran_result[user_id]
        for t in range(len(user_tpl_score)):
            tem_list.append((t, int(user_tpl_score[t]) * user_p))
        result_per_user = sorted(tem_list, key=lambda tem: tem[1], reverse=True)[0:k]
        result_tpl_all = result_tpl_all + result_per_user

    result_filtered = []
    result_map = {}
    for i in range(len(result_tpl_all)):
        tpl_id = result_tpl_all[i][0]
        tpl_score = result_tpl_all[i][1]

        if tpl_id in result_map:
            result_map[tpl_id] = result_map[tpl_id] + tpl_score
        else:
            result_map[tpl_id] = tpl_score

    for i in result_map:
        result_filtered.append((i, result_map[i]))

    res_list = sorted(result_filtered, key=lambda res: res[1], reverse=True)[0:k]
    print(res_list)
    return res_list


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

    over10days = now_time - 100 * 24 * 60 * 60
    over8days = now_time - 80 * 24 * 60 * 60
    over6days = now_time - 60 * 24 * 60 * 60
    over4days = now_time - 40 * 24 * 60 * 60
    over2days = now_time - 20 * 24 * 60 * 60
    # 所有评分
    all_scores = []
    # 模板的评分 key: tname value: user_ksp
    tpls_kps = {}

    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(user_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        # 跳过标题
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


def get_user_name_by_id(user_id):
    for i in range(len(user_list)):
        if user_list[i][1] == user_id:
            return user_list[i][0]
    raise NameError("No such user")


def get_tpl_name_by_id(tpl_id):
    for i in range(len(tpl_list)):
        if tpl_list[i][1] == tpl_id:
            return tpl_list[i][0]
    raise NameError("No such tpl")


def model_score(user_id, res_tpl, predict_time):
    res_tpl_name_list = []

    for res in res_tpl:
        res_tpl_name_list.append(get_tpl_name_by_id(res[0]))

    num_predict_tpl = len(res_tpl_name_list)

    time_f = time.mktime(time.strptime(predict_time, "%Y/%m/%d %H:%M:%S"))

    after_2_time = time_f + 20 * 24 * 60 * 60

    username = get_user_name_by_id(user_id)

    real_preview_tpl = []
    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(user_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        # 去除标题
        next(f_csv)
        for row in f_csv:
            if row[4] == username:
                logtime = time.mktime(time.strptime(row[7], "%Y/%m/%d %H:%M:%S"))
                if time_f < logtime < after_2_time:
                    real_preview_tpl.append(row[1])

    # 去重
    real_preview_tpl = set(real_preview_tpl)
    num_real_preview = len(real_preview_tpl)

    num_precision_tpl = 0

    for res_tpl_name in res_tpl_name_list:
        if res_tpl_name in real_preview_tpl:
            num_precision_tpl += 1
    # 准确率 = 提取出的正确信息条数 /  提取出的信息条数
    p = num_precision_tpl / num_predict_tpl
    # 召回率 = 提取出的正确信息条数 /  样本中的信息条数
    r = num_precision_tpl / num_real_preview
    # f1 F值  = 正确率 * 召回率 * 2 / (正确率 + 召回率) （F 值即为正确率和召回率的调和平均值）
    f1 = p * r * 2 / (p + r)

    print("p: {p}  r: {r} f1: {f1}".format(p=p, r=r, f1=f1))
    return p, r, f1


if __name__ == '__main__':
    main()

    if os.path.exists(user_id_file_path):
        os.remove(user_id_file_path)
    if os.path.exists(tpl_id_file_path):
        os.remove(tpl_id_file_path)
    user_list, tpl_list = allocate_id()

    # result = kps("2017/11/10 00:00:00")
    # if os.path.exists(result_path):
    #     os.remove(result_path)
    # save_to_file(result, result_path)

    # 如果 kps 耗时太长了，直接从文件读上次的结果
    result = read_result()
    top_k_user = get_top_k_near_user(77, 10)
    top_k_tpl = get_top_k_tpl_(top_k_user, 10)
    model_score(77, top_k_tpl, "2017/11/10 00:00:00")

    sys.exit(0)
