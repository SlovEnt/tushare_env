# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/9 13:16'

# %% 导入包
import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
from collections import OrderedDict
import os
import re
import time
from multiprocessing import Pool, Manager
# from  exec_class import Tushare_Proc
from  tushare_exe_class import Tushare_Proc_v2
import traceback
import datetime
import calendar

from chpackage.param_info import get_param_info
BASE_DIR = os.path.dirname(os.getcwd())
CONFIG_INFO_FILE = "%s/%s" % (BASE_DIR, "Config.ini")
PARAINFO = get_param_info(CONFIG_INFO_FILE)

# 引入mysql操作函数
from chpackage import torndb
mysqlExe = torndb.Connection(
    host = "{0}:{1}".format(PARAINFO["DB_HOST"], PARAINFO["DB_PORT"]),
    database = PARAINFO["DB_NAME"],
    user = PARAINFO["USER_NAME"],
    password = PARAINFO["USER_PWD"],
)

pro = ts.pro_api(PARAINFO["TUSHARE_TOKEN"])

# tp = Tushare_Proc(pro, mysqlExe)
# tp = Tushare_Proc(pro, mysqlExe, busiDate="20190106")
tp2 = Tushare_Proc_v2(pro, mysqlExe, busiDate="20190106")

def put_in_db(q, i):
    try:
        isEmpty = False
        while isEmpty is False:
            strSql = q.get()
            print(strSql)
            mysqlExe.execute(strSql)
            # isEmpty = q.empty()
        print(i, "quit")
    except Exception as e:
        print(e, strSql)

def get_tpdatas_new_share(cFlag, q, startDay, endDay):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = ""
    argsDict["start_date"] = startDay
    argsDict["end_date"] = endDay

    strSqlList = tp2.get_tpdatas_new_share(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)

def get_tpdatas_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_daily(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)

def get_tpdatas_weekly(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_weekly(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)

def get_tpdatas_monthly(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_monthly(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)


def get_tpdatas_adj_factor(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_adj_factor(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)


def get_tpdatas_daily_basic(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_daily_basic(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)


def get_tpdatas_suspend(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_suspend(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)



def run_m_get_day_changed_datas(today):

    manage = Manager()
    q = manage.Queue(maxsize=100000)
    p = Pool(10)

    for i in range(5):
        p.apply_async(func=put_in_db, args=(q,i,))

    p.apply_async(func=get_tpdatas_new_share, args=("0", q, today, today))
    p.apply_async(func=get_tpdatas_daily, args=("0", q, today,))
    p.apply_async(func=get_tpdatas_weekly, args=("0", q, today))
    p.apply_async(func=get_tpdatas_monthly, args=("0", q, today))
    p.apply_async(func=get_tpdatas_adj_factor, args=("0", q, today))
    p.apply_async(func=get_tpdatas_daily_basic, args=("0", q, today))
    p.apply_async(func=get_tpdatas_suspend, args=("0", q, today))


if __name__ == '__main__':

    today = time.strftime('%Y%m%d', time.localtime(time.time()))
    run_m_get_day_changed_datas(today)


