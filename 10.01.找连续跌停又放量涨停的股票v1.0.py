# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/12/29 22:24'

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

#/*
#  参数区域
#*/
nDay = 100

def run_down_and_up():
    '''
    思路
    1 nDay 天以内计算标准
    2 出现连续3天及以上跌停
    3 连续出现最少一天有一个比较大的放量 并且第一次放量时间不超过5天
    '''

    # get当日股票列表
    stockCodeList = tp.proc_rtn_datas_stock_code()
    if stockCodeList is False:
        return False

    # for stockCode in stockCodeList:
    #     print(stockCode)

'''

SELECT
	ts_code,
	count(*)
FROM
	daily a
WHERE
	0 = 0
AND pct_chg BETWEEN -11 AND -9
AND trade_date BETWEEN 20181201 AND 20181231
GROUP BY
	ts_code
HAVING
	count(*) > 3
;


'''


if __name__ == '__main__':

    run_down_and_up()


