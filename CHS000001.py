# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/5 21:08'

'''
    基础数据采集
    主要为一次性采集数据 采集前先清空本地表 然后再入库 无日期 股票代码等限制
    
    股票列表
    交易日历
    上市公司基本信息

'''

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




