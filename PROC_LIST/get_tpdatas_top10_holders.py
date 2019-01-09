# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/10 0:02'

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
        while True:
            strSql = q.get()
            print(strSql)
            mysqlExe.execute(strSql)
    except Exception as e:
        print(e, strSql)


def get_tpdatas_top10_holders(cFlag, q, tsCode, tradeCalData):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    argsDict["period"] = tradeCalData

    strSqlList = tp2.get_tpdatas_top10_holders(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)


def run_m_get_news_info():
    manage = Manager()
    q = manage.Queue(maxsize=100000)

    p = Pool(5)

    for i in range(3):
        p.apply_async(func=put_in_db, args=(q,i,))

    tsCodes = tp2.get_datas_for_db_stock_basic()
    tradeCalDatas = tp2.get_datas_for_db_trade_cal("SSE", "20000101", "20190109")

    for tsCode in tsCodes:
        for tradeCalData in tradeCalDatas:
            p.apply_async(func=get_tpdatas_top10_holders, args=("0", q, tsCode["ts_code"], tradeCalData["cal_date"]))

    p.close()
    p.join()


if __name__ == '__main__':

    run_m_get_news_info()

