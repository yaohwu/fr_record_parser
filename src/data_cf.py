# encoding: utf-8
# author: yaoh.wu

import csv
import os
import sys
import time

import numpy as np
from scipy.stats import pearsonr

from data_clean import pure_data_file_path, allocate_id, tpl_id_file_path, user_id_file_path
from utils import save_to_file

# 评分结果文件 列 为模板，行 为用户，1262（模板）*793（用户）
doi_result_file_path = "../data/cf_doi.csv"


def main():
    print("data cf")
    if os.path.exists(user_id_file_path):
        os.remove(user_id_file_path)
    if os.path.exists(tpl_id_file_path):
        os.remove(tpl_id_file_path)
    user_list, tpl_list = allocate_id()

    result = evaluate_doi("2017/11/10 00:00:00", 0.9, tpl_list, user_list)

    if os.path.exists(doi_result_file_path):
        os.remove(doi_result_file_path)
    save_to_file(result, doi_result_file_path)

    top_k_user = get_top_k_near_user(530, 5, result)
    top_k_tpl = get_top_k_tpl(top_k_user, 5, result)
    model_score(530, top_k_tpl, "2017/11/10 00:00:00", tpl_list, user_list)


def evaluate_doi(now_time, tpl_list, user_list):
    """
    计算兴趣程度
    :param user_list: 用户名列表
    :param tpl_list: 模板列表
    :param now_time:计算兴趣程度的时间
    """
    now_time = time.mktime(time.strptime(now_time, "%Y/%m/%d %H:%M:%S"))

    # 过去10，8，6，4，2 天的具体时间 秒（s）
    over10days = now_time - 10 * 5 * 24 * 60 * 60
    over8days = now_time - 8 * 5 * 24 * 60 * 60
    over6days = now_time - 6 * 5 * 24 * 60 * 60
    over4days = now_time - 4 * 5 * 24 * 60 * 60
    over2days = now_time - 2 * 5 * 24 * 60 * 60
    # 过去10。8，6，4，2 天一次预览对应的分数
    over10_p = 1
    over8_p = 2
    over6_p = 3
    over4_p = 4
    over2_p = 5

    # 所有评分
    all_doi = []
    # 模板的评分 {key:tname value:user_doi{key:user value: doi}}
    all_tpl_kps = {}

    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(pure_data_file_path, encoding="utf-8", newline="") as f:
        f_csv = csv.reader(f, dialect="excel")
        # 跳过标题
        next(f_csv)
        for row in f_csv:
            # 模板名
            tname = row[1]
            # 用户名
            username = row[4]

            # logtime
            logtime = row[7]

            logtime = time.mktime(time.strptime(logtime, "%Y/%m/%d %H:%M:%S"))

            if logtime < over10days:
                score = 0
            elif over8days > logtime > over10days:
                score = over10_p
            elif over6days > logtime > over8days:
                score = over8_p
            elif over4days > logtime > over6days:
                score = over6_p
            elif over2days > logtime > over4days:
                score = over4_p
            elif now_time > logtime > over2days:
                score = over2_p
            else:
                # 由于数据是时间有序的，因此如果logtime > now_time 就可以直接跳出了
                break
            # 存储分数
            if tname in all_tpl_kps:
                if username in all_tpl_kps[tname]:
                    all_tpl_kps[tname][username] = max(all_tpl_kps[tname][username], score)
                else:
                    all_tpl_kps[tname][username] = score
            else:
                all_tpl_kps[tname] = {}
                all_tpl_kps[tname][username] = score

    for tpl in tpl_list:
        tpl_score = []
        # 模板是否有被预览过
        if tpl[0] in all_tpl_kps.keys():
            tpl_kps = all_tpl_kps[tpl[0]]
            for user in user_list:
                # 用户是否预览过该模板
                if user[0] in tpl_kps.keys():
                    tpl_score.append(tpl_kps[user[0]])
                else:
                    tpl_score.append(0)
        else:
            # 模板在这段时间内没被预览过，所有用户对他的兴趣程度为0
            tpl_score = [0] * len(user_list)

        all_doi.append(tpl_score)

    # 矩阵转置 从 793（用户）*1262（模板）转换成 1262（模板）*793（用户）
    all_doi = np.transpose(all_doi)
    return all_doi


def get_top_k_near_user(user_id, k, all_doi):
    users_p = []
    for i in range(len(all_doi)):
        user_a = all_doi[user_id]
        user_b = all_doi[i]

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


def get_top_k_tpl(result_user, k, all_doi):
    result_tpl_all = []

    for i in range(len(result_user)):
        user_id = result_user[i][0]
        user_p = result_user[i][1]

        tem_list = []
        user_tpl_score = all_doi[user_id]
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


def get_user_name_by_id(user_id, user_list):
    for i in range(len(user_list)):
        if user_list[i][1] == user_id:
            return user_list[i][0]
    raise NameError("No such user")


def get_tpl_name_by_id(tpl_id, tpl_list):
    for i in range(len(tpl_list)):
        if tpl_list[i][1] == tpl_id:
            return tpl_list[i][0]
    raise NameError("No such tpl")


def model_score(user_id, res_tpl, predict_time, tpl_list, user_list):
    res_tpl_name_list = []

    for res in res_tpl:
        res_tpl_name_list.append(get_tpl_name_by_id(res[0], tpl_list))

    num_predict_tpl = len(res_tpl_name_list)

    time_f = time.mktime(time.strptime(predict_time, "%Y/%m/%d %H:%M:%S"))

    after_2_time = time_f + 2 * 5 * 24 * 60 * 60

    username = get_user_name_by_id(user_id, user_list)

    real_preview_tpl = []
    # id,tname,type,ip,username,userrole,time,logtime,memory
    with open(pure_data_file_path, encoding="utf-8", newline="") as f:
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
    sys.exit(0)
