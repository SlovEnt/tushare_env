# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/5 22:45'

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
tp = Tushare_Proc(pro, mysqlExe)

def proc_main_stock_basic_datas():
    argsDict = {}
    argsDict["inputCode"] = ""
    argsDict["codeType"] = ""
    argsDict["recollect"] = "0"
    tp.proc_main_stock_basic_datas(argsDict)

def proc_main_trade_cal_datas():
    tp.proc_main_trade_cal_datas()

def proc_main_stock_company_datas():
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "exchange"

    argsDict["inputCode"] = "SSE"
    tp.proc_main_stock_company_datas(argsDict)

    argsDict["inputCode"] = "SZSE"
    tp.proc_main_stock_company_datas(argsDict)

def proc_main_namechange_datas():
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = ""
    argsDict["inputCode"] = ""
    tp.proc_main_namechange_datas(argsDict)

def proc_main_hs_const_datas():
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "hs_type"

    argsDict["inputCode"] = "SH"
    tp.proc_main_hs_const_datas(argsDict)

    argsDict["inputCode"] = "SZ"
    tp.proc_main_hs_const_datas(argsDict)

def proc_main_new_share_datas():
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = ""
    argsDict["inputCode"] = ""
    tp.proc_main_new_share_datas(argsDict)

def proc_main_concept_datas():
    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "src"
    argsDict["inputCode"] = "ts"
    tp.proc_main_concept_datas(argsDict)

def proc_main_index_basic_datas():
    sysDicts = tp.get_datas_for_db_sys_dict("market")
    for sysDict in sysDicts:
        argsDict = {}
        argsDict["recollect"] = "0"
        argsDict["codeType"] = "market"
        argsDict["inputCode"] = sysDict["dict_item"]
        tp.proc_main_index_basic_datas(argsDict)

def proc_main_fund_basic_datas():

    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = "market"
    argsDict["inputCode"] = "E"
    tp.proc_main_fund_basic_datas(argsDict)
    argsDict["inputCode"] = "O"
    tp.proc_main_fund_basic_datas(argsDict)

def proc_main_fund_company_datas():

    argsDict = {}
    argsDict["recollect"] = "0"
    argsDict["codeType"] = ""
    argsDict["inputCode"] = ""
    tp.proc_main_fund_company_datas(argsDict)




def run_main():

    p = Pool(5)

    p.apply_async(proc_main_stock_basic_datas,)
    p.apply_async(proc_main_trade_cal_datas,)
    p.apply_async(proc_main_stock_company_datas,)
    p.apply_async(proc_main_namechange_datas,)
    p.apply_async(proc_main_hs_const_datas,)
    p.apply_async(proc_main_new_share_datas,)
    p.apply_async(proc_main_concept_datas,)
    p.apply_async(proc_main_index_basic_datas,)
    p.apply_async(proc_main_fund_basic_datas,)
    p.apply_async(proc_main_fund_company_datas,)

    p.close()
    p.join()


if __name__ == '__main__':

    run_main()

    # proc_main_namechange_datas()



