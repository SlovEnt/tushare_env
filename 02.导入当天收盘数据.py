# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/12/25 20:15'

# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/12/25 20:15'

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

def imp_by_stock_code_datas_2_db():

    stockCodeList = tp.get_datas_for_db_stock_basic()
    if stockCodeList is False:
        return False

    for stockCode in stockCodeList:

        argsDict = {}
        argsDict["inputCode"] = stockCode["ts_code"]
        argsDict["codeType"] = "ts_code"
        tp.proc_main_adj_factor_datas(argsDict)
        tp.proc_main_suspend_datas(argsDict)
        tp.proc_main_daily_basic_datas(argsDict)
        tp.proc_main_income_datas(argsDict)
        tp.proc_main_balancesheet_datas(argsDict)
        tp.proc_main_cashflow_datas(argsDict)
        tp.proc_main_forecast_datas(argsDict)
        tp.proc_main_express_datas(argsDict)
        tp.proc_main_dividend_datas(argsDict)
        tp.proc_main_fina_indicator_datas(argsDict)
        tp.proc_main_fina_audit_datas(argsDict)
        tp.proc_main_fina_mainbz_datas(argsDict)
        tp.proc_main_hsgt_top10_datas(argsDict)
        tp.proc_main_top10_holders_datas(argsDict)
        tp.proc_main_top10_floatholders_datas(argsDict)
        tp.proc_main_top_list_datas(argsDict)
        tp.proc_main_pledge_stat_datas(argsDict)
        tp.proc_main_pledge_detail_datas(argsDict)

def run():
    pass

def main():

    # 按trade_date导入数据 导入当日数据
    argsDict = {}
    argsDict["recollect"] = "1" # 强制重采标识 0 不强制 1 强制
    yyyyMmDd = time.strftime("%Y%m%d")
    argsDict["inputCode"] = yyyyMmDd
    argsDict["inputCode"] = "20181224"

    argsDict["codeType"] = "trade_date"
    # tp.proc_main_daily_basic_datas(argsDict)
    # tp.proc_main_adj_factor_datas(argsDict)
    # tp.proc_main_moneyflow_hsgt_datas(argsDict)
    # tp.proc_main_hsgt_top10_datas(argsDict)
    # tp.proc_main_ggt_top10_datas(argsDict)
    # tp.proc_main_margin_detail_datas(argsDict)
    # tp.proc_main_margin_datas(argsDict)
    # tp.proc_main_top_list_datas(argsDict)
    tp.proc_main_top_inst_datas(argsDict)



    # argsDict["codeType"] = "suspend_date"
    # tp.proc_main_suspend_datas(argsDict)

    # 利润表 必须通过股票代码获取
    # tp.proc_main_income_datas(argsDict)
    # tp.proc_main_balancesheet_datas(argsDict)
    # tp.proc_main_cashflow_datas(argsDict)
    # tp.proc_main_forecast_datas(argsDict)
    # tp.proc_main_express_datas(argsDict)
    # tp.proc_main_dividend_datas(argsDict)
    # tp.proc_main_fina_indicator_datas(argsDict)
    # tp.proc_main_fina_audit_datas(argsDict)
    # tp.proc_main_fina_mainbz_datas(argsDict)
    # tp.proc_main_top10_holders_datas(argsDict)
    # tp.proc_main_pledge_stat_datas(argsDict)
    # tp.proc_main_pledge_detail_datas(argsDict)

    # tp.proc_main_top10_floatholders_datas(argsDict)


    # imp_by_stock_code_datas_2_db()


if __name__ == '__main__':
    main()