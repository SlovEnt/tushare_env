# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/9 12:55'

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

def gen_year_month_list(starYear, endYear):

    dateInfoList = []

    for year in range(starYear, endYear+1):
        for month in range(1, 13):

            dateInfo = OrderedDict()

            startDay, endDay = calendar.monthrange(year, month)
            startDate = "%s%02d%s" % (year, month, "01")
            endDate = "%s%02d%s" % (year, month, endDay)
            comparYearMonth = "%s%02d" % (year, month)

            dateInfo["start_date"] = startDate
            dateInfo["end_date"] = endDate

            # 如果超过当前月份 则不需要继续获取
            nowYearMonth = time.strftime("%Y%m")
            if comparYearMonth > nowYearMonth:
                continue
            dateInfoList.append(dateInfo)

    return dateInfoList

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

def run_m_get_month_datas():
    manage = Manager()
    q = manage.Queue(maxsize=100000)

    p = Pool(10)

    for i in range(5):
        p.apply_async(func=put_in_db, args=(q,i,))

    # 第一个入参明确是否强制重新采集 0 采集后不重采 1 强制重采
    # 根据起始年份到结束年份，每个月为一个采集序列 一次采集一个月的数据
    starYear = 1990
    endYear = 2019

    dateYMDList = gen_year_month_list(starYear, endYear)
    for dateYMD in dateYMDList:
        p.apply_async(func=get_tpdatas_new_share, args=("0", q, dateYMD["start_date"], dateYMD["end_date"]))

    p.close()
    p.join()


if __name__ == '__main__':

    run_m_get_month_datas()

