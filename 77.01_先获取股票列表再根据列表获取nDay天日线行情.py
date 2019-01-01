# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/1 16:00'

# %% 导入包
import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
from collections import OrderedDict
import os
import re
import time
from multiprocessing import Pool
from  exec_class import Tushare_Proc
import traceback


from chpackage.param_info import get_param_info
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
tp = Tushare_Proc(pro, mysqlExe)


def get_trade_cal():
    tradeCalInfo = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
    datas = tradeCalInfo.to_dict("records")
    return datas


if __name__ == '__main__':
    tradeCalDict = get_trade_cal()

    for tradeCalInfo in tradeCalDict:
        if tradeCalInfo["is_open"] == 0:
            continue

        tp.proc_main_daily_datas(tradeCalInfo["cal_date"])

    # insert_ts_daily_2_db()

