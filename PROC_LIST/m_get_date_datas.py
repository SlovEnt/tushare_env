# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/7 16:06'

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

def get_tpdatas_new_share(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "start_date"
    argsDict["inputCode"] = date
    argsDict["start_date"] = date
    argsDict["end_date"] = date

    strSqlList = tp2.get_tpdatas_new_share(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_index_dailybasic(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_index_dailybasic(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_moneyflow_hsgt(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_moneyflow_hsgt(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_hsgt_top10(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_hsgt_top10(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_ggt_top10(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_ggt_top10(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_margin(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_margin(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_margin_detail(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_margin_detail(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_top_list(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_top_list(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_top_inst(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_top_inst(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_repurchase(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "ann_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_repurchase(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fut_holding(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_fut_holding(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fut_wsr(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_fut_wsr(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_fut_settle(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "trade_date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_fut_settle(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_shibor(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_shibor(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_shibor_quote(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_shibor_quote(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_shibor_lpr(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_shibor_lpr(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_libor(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_libor(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_hibor(cFlag, q, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date

    strSqlList = tp2.get_tpdatas_hibor(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_tmt_twincomedetail(cFlag, q, item, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date
    argsDict["item"] = item
    argsDict["date"] = date

    strSqlList = tp2.get_tpdatas_tmt_twincomedetail(argsDict)
    if len(strSqlList) != 0:
        for strSql in strSqlList:
            q.put(strSql)

def get_tpdatas_tmt_twincome(cFlag, q, item, date):
    argsDict = {}
    argsDict["recollect"] = cFlag
    argsDict["codeType"] = "date"
    argsDict["inputCode"] = date
    argsDict["item"] = item
    argsDict["date"] = date

    strSqlList = tp2.get_tpdatas_tmt_twincome(argsDict)
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

    tradeCalDatas = tp2.get_datas_for_db_trade_cal("SSE", "20180701", "20180731")

    for i in range(8):
        p.apply_async(func=put_in_db, args=(q,i,))

    # 第一个入参明确是否强制重新采集 0 采集后不重采 1 强制重采

    for tradeCalData in tradeCalDatas:
        # print(tradeCalData["cal_date"])

        # p.apply_async(func=get_tpdatas_index_dailybasic, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_new_share, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_moneyflow_hsgt, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_hsgt_top10, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_ggt_top10, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_margin, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_margin_detail, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_top_list, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_top_inst, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_repurchase, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_fut_holding, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_fut_wsr, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_fut_settle, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_shibor, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_shibor_quote, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_shibor_lpr, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_libor, args=("0", q, tradeCalData["cal_date"]))
        # p.apply_async(func=get_tpdatas_hibor, args=("0", q, tradeCalData["cal_date"]))

        sysDicts = tp2.get_datas_for_db_sys_dict("item")
        for sysDict in sysDicts:
            p.apply_async(func=get_tpdatas_tmt_twincome, args=("0", q, sysDict["dict_item"], tradeCalData["cal_date"]))

            p.apply_async(func=get_tpdatas_tmt_twincomedetail, args=("0", q, sysDict["dict_item"], tradeCalData["cal_date"]))

    #     # p.apply_async(func=get_tpdatas_weekly, args=("1",q, tsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_monthly, args=("1",q, tsCode["ts_code"]))
    #     # p.apply_async(func=get_tpdatas_adj_factor, args=("1",q, tsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_daily_basic, args=("1",q, tsCode["ts_code"]))
    #     p.apply_async(func=get_tpdatas_suspend, args=("1",q, tsCode["ts_code"]))


    p.close()
    p.join()


if __name__ == '__main__':


    run_m_get_tscode_datas()

