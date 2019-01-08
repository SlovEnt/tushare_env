# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/7 16:06'

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

def run_m_get_tscode_datas():
    manage = Manager()
    q = manage.Queue(maxsize=100000)

    p = Pool(15)

    tradeCalDatas = tp2.get_datas_for_db_trade_cal("19000101", "20190106")

    for i in range(8):
        p.apply_async(func=put_in_db, args=(q,i,))

    # 第一个入参明确是否强制重新采集 0 采集后不重采 1 强制重采

    for tradeCalData in tradeCalDatas:
        print(tradeCalData["cal_date"])
    #     # p.apply_async(func=get_tpdatas_daily, args=("1",q, tsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_weekly, args=("1",q, tsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_monthly, args=("1",q, tsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_adj_factor, args=("1",q, tsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_daily_basic, args=("1",q, tsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_suspend, args=("1",q, tsCode["ts_code"]))


    p.close()
    p.join()


if __name__ == '__main__':

    run_m_get_tscode_datas()

