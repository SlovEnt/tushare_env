# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/7 10:50'

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

def get_tpdatas_stock_basic(cFlag, q):

    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["inputCode"] = ""
    argsDict["codeType"] = ""

    strSqlList = tp2.get_tpdatas_stock_basic(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)

def get_tpdatas_stock_company(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "exchange"

    argsDict["inputCode"] = "SSE"
    strSqlList = tp2.get_tpdatas_stock_company(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

    argsDict["inputCode"] = "SZSE"
    strSqlList = tp2.get_tpdatas_stock_company(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_hs_const(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "hs_type"

    argsDict["inputCode"] = "SH"
    strSqlList = tp2.get_tpdatas_hs_const(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

    argsDict["inputCode"] = "SZ"
    strSqlList = tp2.get_tpdatas_hs_const(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_namechange(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = ""
    argsDict["inputCode"] = ""

    strSqlList = tp2.get_tpdatas_namechange(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_new_share(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = ""

    strSqlList = tp2.get_tpdatas_new_share(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_concept(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "src"
    argsDict["inputCode"] = "ts"

    strSqlList = tp2.get_tpdatas_concept(argsDict)

    if len(strSqlList) == 0:
        return

    for strSql in strSqlList:
        q.put(strSql)

def get_tpdatas_index_basic(cFlag, q, inputCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "market"
    argsDict["inputCode"] = inputCode

    strSqlList = tp2.get_tpdatas_index_basic(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_basic(cFlag, q, inputCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "market"
    argsDict["inputCode"] = inputCode

    strSqlList = tp2.get_tpdatas_fund_basic(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fut_basic(cFlag, q, inputCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "exchange"
    argsDict["inputCode"] = inputCode

    strSqlList = tp2.get_tpdatas_fut_basic(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_opt_basic(cFlag, q, inputCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "exchange"
    argsDict["inputCode"] = inputCode

    strSqlList = tp2.get_tpdatas_opt_basic(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_company(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = ""
    argsDict["inputCode"] = ""

    strSqlList = tp2.get_tpdatas_fund_company(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)


def put_in_db(q, i):
    try:
        while True:
            strSql = q.get()
            print(strSql)
            mysqlExe.execute(strSql)
    except Exception as e:
        print(e, strSql)

def run_m_get_all_basic_datas():

    manage = Manager()
    q = manage.Queue(maxsize=100000)

    p = Pool(15)

    for i in range(8):
        p.apply_async(func=put_in_db, args=(q,i,))

    # 第一个入参明确是否强制重新采集 0 采集后不重采 1 强制重采
    p.apply_async(func=get_tpdatas_stock_basic, args=("0",q,))
    p.apply_async(func=get_tpdatas_stock_company, args=("0",q,))
    p.apply_async(func=get_tpdatas_namechange, args=("0",q,))
    p.apply_async(func=get_tpdatas_hs_const, args=("0",q,))
    p.apply_async(func=get_tpdatas_new_share, args=("0",q,))
    p.apply_async(func=get_tpdatas_concept, args=("0",q,))

    sysDicts = tp2.get_datas_for_db_sys_dict("market")
    for sysDict in sysDicts:
        p.apply_async(func=get_tpdatas_index_basic, args=("0", q, sysDict["dict_item"]))

    p.apply_async(func=get_tpdatas_fund_basic, args=("0", q, "E"))
    p.apply_async(func=get_tpdatas_fund_basic, args=("0", q, "O"))
    p.apply_async(func=get_tpdatas_fund_company, args=("0", q))


    # 期货合约信息表
    exchangeCodeList = ["CFFEX","DCE","CZCE","SHFE","INE"]
    for exchangeCode in exchangeCodeList:
        p.apply_async(func=get_tpdatas_fut_basic, args=("0", q, exchangeCode))

    # 期权合约
    exchangeCodeList = ["SSE"]
    for exchangeCode in exchangeCodeList:
        p.apply_async(func=get_tpdatas_opt_basic, args=("1", q, exchangeCode))

    p.close()
    p.join()


if __name__ == '__main__':

    # 获取各交易所的交易日历，每年年底再打开获取打开获取
    # exchangeCodeList = tp2.get_datas_for_db_sys_dict("exchange")
    # for exchangeCode in exchangeCodeList:
    #     tp2.get_tpdatas_trade_cal_2_db(exchangeCode["dict_item"],"20191201","20101231")

    run_m_get_all_basic_datas()
