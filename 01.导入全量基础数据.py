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

def imp_basic_datas_2_db():
    ''' 该函数集合执行的是无参数或固定参数的数据导入 '''
    tp.proc_main_stock_basic_datas()
    tp.proc_main_trade_cal_datas()
    tp.proc_main_stock_company_datas()
    tp.proc_main_hs_const_datas("SH")
    tp.proc_main_hs_const_datas("SZ")
    tp.proc_main_new_share_datas()
    tp.proc_main_fund_company_datas()
    tp.proc_main_concept_datas({"inputCode": "ts", "codeType": "src"})
    tp.proc_main_fund_basic_datas({"inputCode": "E", "codeType": "market"})
    tp.proc_main_fund_basic_datas({"inputCode": "O", "codeType": "market"})

def main():

    imp_basic_datas_2_db()

    # pool = Pool(10)
    #
    #
    #
    # pool.close()
    # pool.join()


if __name__ == '__main__':
    main()