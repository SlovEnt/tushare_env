# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/7 15:34'

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
tp2 = Tushare_Proc_v2(pro, mysqlExe, busiDate="20190109")


def get_tpdatas_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_daily(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_weekly(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_weekly(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_monthly(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_monthly(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_adj_factor(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_adj_factor(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_daily_basic(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_daily_basic(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_suspend(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_suspend(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_income(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_income(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_balancesheet(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_balancesheet(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_cashflow(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_cashflow(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_forecast(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_forecast(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_express(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_express(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_dividend(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_dividend(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fina_indicator(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fina_indicator(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fina_audit(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fina_audit(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fina_mainbz(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fina_mainbz(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_share_float(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_share_float(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_block_trade(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_block_trade(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_index_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_index_daily(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_index_weight(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "index_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_index_weight(argsDict)

    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_nav(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fund_nav(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_div(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fund_div(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_portfolio(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fund_portfolio(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fund_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fund_daily(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fut_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_fut_daily(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_opt_daily(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_opt_daily(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_index_dailybasic(cFlag, q, tsCode):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode

    strSqlList = tp2.get_tpdatas_index_dailybasic(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_concept_detail(cFlag, q):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "id"
    conceptCodeList = tp2.get_datas_for_db_concept()

    for conceptCode in conceptCodeList:
        argsDict["inputCode"] = conceptCode["code"]
        strSqlList = tp2.get_tpdatas_concept_detail(argsDict)

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

def run_m_get_tscode_datas():
    manage = Manager()
    q = manage.Queue(maxsize=100000)

    p = Pool(15)

    tsCodes = tp2.get_datas_for_db_stock_basic()

    for i in range(8):
        p.apply_async(func=put_in_db, args=(q,i,))

    # 第一个入参明确是否强制重新采集 0 采集后不重采 1 强制重采

    for tsCode in tsCodes:

        # 行情数据
        # p.apply_async(func=get_tpdatas_daily, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_weekly, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_monthly, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_adj_factor, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_daily_basic, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_suspend, args=("0",q, tsCode["ts_code"]))

        # 财务数据
        # p.apply_async(func=get_tpdatas_income, args=("0",q, tsCode["ts_code"]))
        p.apply_async(func=get_tpdatas_balancesheet, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_cashflow, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_forecast, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_express, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_dividend, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_fina_indicator, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_fina_audit, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_fina_mainbz, args=("0",q, tsCode["ts_code"]))

        # 市场参考数据
        # p.apply_async(func=get_tpdatas_share_float, args=("0",q, tsCode["ts_code"]))
        # p.apply_async(func=get_tpdatas_block_trade, args=("0",q, tsCode["ts_code"]))

        pass

    # p.apply_async(func=get_tpdatas_concept_detail, args=("0",q,))

    # --- 指数日线行情
    # marketTsCodeList = tp2.get_datas_for_db_index_basic()
    # for marketTsCode in marketTsCodeList:
    #     # p.apply_async(func=get_tpdatas_index_daily, args=("0", q, marketTsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_index_weight, args=("0", q, marketTsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_index_dailybasic, args=("0", q, marketTsCode["ts_code"]))

    # 基金数据
    # fundTsCodeList = tp2.get_datas_for_db_fund_basic()
    # for fundTsCode in fundTsCodeList:
    #     p.apply_async(func=get_tpdatas_fund_nav, args=("0", q, fundTsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_fund_div, args=("0", q, fundTsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_fund_portfolio, args=("0", q, fundTsCode["ts_code"])) # 1000积分才允许调用
    #     p.apply_async(func=get_tpdatas_fund_daily, args=("0", q, fundTsCode["ts_code"]))

    # 期货日线行情
    # futTsCodeList = tp2.get_datas_for_db_fut_basic()
    # for futTsCode in futTsCodeList:
    #     p.apply_async(func=get_tpdatas_fut_daily, args=("0", q, futTsCode["ts_code"]))

    # 期权日线行情
    optTsCodeList = tp2.get_datas_for_db_opt_basic()
    for optTsCode in optTsCodeList:
        p.apply_async(func=get_tpdatas_opt_daily, args=("0", q, optTsCode["ts_code"]))




    p.close()
    p.join()


if __name__ == '__main__':

    run_m_get_tscode_datas()

