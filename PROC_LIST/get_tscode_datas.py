# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/6 0:35'

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
tp = Tushare_Proc(pro, mysqlExe, busiDate="20190106")

def proc_main_daily_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_daily_datas(argsDict)

def proc_main_daily_basic_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_daily_basic_datas(argsDict)

def proc_main_weekly_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_weekly_datas(argsDict)

def proc_main_monthly_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_monthly_datas(argsDict)

def proc_main_adj_factor_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_adj_factor_datas(argsDict)

def proc_main_suspend_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_suspend_datas(argsDict)

def proc_main_income_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_income_datas(argsDict)

def proc_main_balancesheet_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_balancesheet_datas(argsDict)

def proc_main_cashflow_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_cashflow_datas(argsDict)

def proc_main_forecast_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_forecast_datas(argsDict)

def proc_main_express_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_express_datas(argsDict)

def proc_main_dividend_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_dividend_datas(argsDict)

def proc_main_fina_indicator_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_fina_indicator_datas(argsDict)

def proc_main_fina_audit_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_fina_audit_datas(argsDict)

def proc_main_fina_mainbz_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_fina_mainbz_datas(argsDict)

def proc_main_hsgt_top10_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_hsgt_top10_datas(argsDict)

def proc_main_ggt_top10_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_ggt_top10_datas(argsDict)

def proc_main_top10_holders_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_top10_holders_datas(argsDict)

def proc_main_top10_floatholders_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_top10_floatholders_datas(argsDict)

def proc_main_pledge_stat_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_pledge_stat_datas(argsDict)

def proc_main_pledge_detail_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_pledge_detail_datas(argsDict)

def proc_main_share_float_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_share_float_datas(argsDict)

def proc_main_block_trade_datas(tsCode):
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "ts_code"
    argsDict["inputCode"] = tsCode
    tp.proc_main_block_trade_datas(argsDict)


def run_main():

    tsCodes = tp.get_datas_for_db_stock_basic()

    p = Pool(6)

    for tsCode in tsCodes:

        # proc_main_income_datas(tsCode["ts_code"])

        p.apply_async(proc_main_daily_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_daily_basic_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_weekly_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_monthly_datas, args=(tsCode["ts_code"],) )

        # p.apply_async(proc_main_adj_factor_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_suspend_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_income_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_balancesheet_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_cashflow_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_forecast_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_express_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_dividend_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_fina_indicator_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_fina_audit_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_fina_mainbz_datas, args=(tsCode["ts_code"],) )
        #
        # p.apply_async(proc_main_hsgt_top10_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_ggt_top10_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_top10_holders_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_top10_floatholders_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_pledge_stat_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_pledge_detail_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_share_float_datas, args=(tsCode["ts_code"],) )
        # p.apply_async(proc_main_block_trade_datas, args=(tsCode["ts_code"],) )

    p.close()
    p.join()

if __name__ == '__main__':

    run_main()
