# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/5 19:37'

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

def run_man(conceptCode):
    argsDict = {"recollect": "0", "inputCode": "{0}".format(conceptCode), "codeType": "id"}
    tp.proc_main_concept_detail_datas(argsDict)

if __name__ == '__main__':

    tp.proc_main_concept_datas({"recollect": "0", "inputCode": "", "codeType": "src"})

    conceptCodeList = tp.get_datas_for_db_concept()

    pool = Pool(3)

    for conceptCode in conceptCodeList:
        print()
        pool.apply_async(run_man, args=(conceptCode["code"],))
        # tp.proc_main_concept_detail_datas({"recollect": "1", "inputCode": "{0}".format(conceptCode["code"]), "codeType": "id"})

    pool.close()
    pool.join()

