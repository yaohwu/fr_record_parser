# encoding: utf-8
# author: yaoh.wu

import csv
import os
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import pearsonr

from data_clean import pure_data_file_path, allocate_id, tpl_id_file_path, user_id_file_path
from utils import save_to_file

# 评分结果文件 列 为模板，行 为用户，1262（模板）*793（用户）
doi_result_file_path = "../data/cf_doi.csv"


def main():
    print("data cf")

    # 待预测用户id
    user_id = 77

    # t
    t = 30
    # time
    predict_time = "2017/11/10 00:00:00"

    if os.path.exists(user_id_file_path):
        os.remove(user_id_file_path)
    if os.path.exists(tpl_id_file_path):
        os.remove(tpl_id_file_path)
    user_list, tpl_list = allocate_id()

    result, hots = evaluate_doi(predict_time, tpl_list, user_list, t)
    user_name = get_user_name_by_id(user_id, user_list)

    x = []
    yp1 = []
    yr1 = []
    yp2 = []
    yr2 = []
    yp3 = []
    yr3 = []

    history_list = common_predict(predict_time, t, user_name, tpl_list, hots)
    for i in range(1, 20):
        x.append(i)
        print("k: {k}".format(k=i))
        k = i
        if os.path.exists(doi_result_file_path):
            os.remove(doi_result_file_path)
        save_to_file(result, doi_result_file_path)

        # # 使用皮尔逊相关系数计算用户间的相似度
        # print("pearson:")
        # top_k_user = get_top_k_near_user(evaluate_pearson, user_id, k + 1, result)
        # top_k_tpl = get_top_k_tpl(top_k_user, k, result)
        # p1, r1, f11 = model_score(user_id, top_k_tpl, predict_time, tpl_list, user_list)
        # yp1.append(p1)
        # yr1.append(r1)
        #
        # # 使用 Jaccard 相似度计算用户间的相似度
        # print("jaccard:")
        # top_k_user = get_top_k_near_user(evaluate_jaccard, user_id, k + 1, result)
        # top_k_tpl = get_top_k_tpl(top_k_user, k, result)
        # p2, r2, f12 = model_score(user_id, top_k_tpl, predict_time, tpl_list, user_list)
        # yp2.append(p2)
        # yr2.append(r2)

        print("common:")
        top_k_tpl = sorted(history_list, key=lambda h: h[1], reverse=True)[0: k]
        p3, r3, f13 = model_score(user_id, top_k_tpl, predict_time, tpl_list, user_list)
        yp3.append(p3)
        yr3.append(r3)
        print("\n")

    # plt.plot(x, yp1, label='pearson p')
    # plt.plot(x, yp2, label='jaccard p')
    # plt.plot(x, yr1, label="pearson r")
    # plt.plot(x, yr2, label="jaccard r")

    plt.plot(x, yp3, label='common p')
    plt.plot(x, yr3, label='common r')
    plt.xlabel('k')
    plt.ylabel('p/r')
    plt.title("k pr")
    plt.legend()
    plt.show()


def common_predict(now_time, t, username_predict, tpl_list, hots):
    now_time = time.mktime(time.strptime(now_time, "%Y/%m/%d %H:%M:%S"))
    # 过去t天的具体时间 秒（s）
    over_t_days = now_time - t * 24 * 60 * 60
    history = {}
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

            if logtime < over_t_days:
                continue
            elif now_time > logtime > over_t_days:
                if username == username_predict:
                    if tname in history.keys():
                        history[tname] += 1
                    else:
                        history[tname] = 0
            else:
                # 由于数据是时间有序的，因此如果logtime > now_time 就可以直接跳出了
                break
    history_list = []
    for t in range(len(tpl_list)):
        tpl = tpl_list[t]
        if tpl[0] in history.keys():
            history_list.append((tpl[1], history[tpl[0]] / hots[tpl[1]]))
    return history_list


def evaluate_doi(now_time, tpl_list, user_list, t):
    """
    计算兴趣程度
    :param t: 计算的数据来源与前t天
    :param user_list: 用户名列表
    :param tpl_list: 模板列表
    :param now_time:计算兴趣程度的时间
    """
    now_time = time.mktime(time.strptime(now_time, "%Y/%m/%d %H:%M:%S"))

    # 过去t天的具体时间 秒（s）
    over_t_days = now_time - t * 24 * 60 * 60
    p_score = 1

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

            if logtime < over_t_days:
                continue
            elif now_time > logtime > over_t_days:
                score = p_score
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
    tpls_hot = []
    for i in range(len(tpl_list)):
        tpl = tpl_list[i]
        tpl_score = []
        tpl_hot = 0
        # 模板是否有被预览过
        if tpl[0] in all_tpl_kps.keys():
            tpl_kps = all_tpl_kps[tpl[0]]
            for user in user_list:
                # 用户是否预览过该模板
                if user[0] in tpl_kps.keys():
                    tpl_score.append(tpl_kps[user[0]])
                    if tpl_kps[user[0]] != 0:
                        tpl_hot += 1
                else:
                    tpl_score.append(0)
        else:
            # 模板在这段时间内没被预览过，所有用户对他的兴趣程度为0
            tpl_score = [0] * len(user_list)

        all_doi.append(tpl_score)
        tpls_hot.append(tpl_hot)

    # 矩阵转置 从 793（用户）*1262（模板）转换成 1262（模板）*793（用户）
    all_doi = np.transpose(all_doi)
    return all_doi, tpls_hot


def evaluate_jaccard(all_doi, user_id):
    """
    改进的jaccard 相似度
    :param all_doi:
    :param user_id:
    :return:
    """
    users_p = []

    for i in range(len(all_doi)):
        user_a = all_doi[user_id]
        user_b = all_doi[i]

        if user_id == i:
            continue
        union = 0
        intersect = 0
        for inner in range(len(user_a)):
            a = int(user_a[inner])
            b = int(user_b[inner])
            if a != 0 or b != 0:
                union += 1
                if a != 0 and b != 0:
                    intersect += 1
        if union == 0:
            p = 0
        else:
            p = intersect / union
        users_p.append((i, p))
    return users_p


def evaluate_pearson(all_doi, user_id):
    """
    计算用户间的皮尔逊相关系数
    :param all_doi: 兴趣程度矩阵
    :param user_id: 用户id
    :return: 用户相似度列表
    """
    users_p = []
    for i in range(len(all_doi)):
        user_a = all_doi[user_id]
        user_b = all_doi[i]

        if user_id == i:
            continue

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
    return users_p


def get_top_k_near_user(strategy, user_id, k, all_doi, hots=None):
    if hots is None:
        users_p = strategy(all_doi, user_id)
    else:
        users_p = strategy(all_doi, user_id, hots)
    result_user = sorted(users_p, key=lambda user_p: user_p[1], reverse=True)[0:k]
    print(result_user)
    return result_user


def get_top_k_tpl(result_user, k, all_doi):
    result_tpl_all = []
    p_last = result_user[-1][1]
    for i in range(len(result_user)):
        user_id = result_user[i][0]
        user_p = result_user[i][1]

        tem_list = []
        user_tpl_score = all_doi[user_id]
        for t in range(len(user_tpl_score)):
            tem_list.append((t, int(user_tpl_score[t]) * (user_p - p_last)))
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
    print(res_tpl_name_list)
    num_predict_tpl = len(res_tpl_name_list)

    time_f = time.mktime(time.strptime(predict_time, "%Y/%m/%d %H:%M:%S"))

    after_2_time = time_f + 5 * 24 * 60 * 60

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
    try:
        f1 = p * r * 2 / (p + r)
    except ZeroDivisionError:
        f1 = 0

    print("p: {p}  r: {r} f1: {f1}".format(p=p, r=r, f1=f1))
    return p, r, f1


if __name__ == '__main__':
    main()
    sys.exit(0)
