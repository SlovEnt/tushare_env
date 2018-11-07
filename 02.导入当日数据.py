# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/4 22:38'

from collections import OrderedDict
import os
import re
import time
import tushare as ts
from multiprocessing import Pool
import traceback

from chpackage.param_info import get_param_info
from chpackage import torndb
from  exec_class import Tushare_Proc


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_INFO_FILE = "%s/%s" % (BASE_DIR, "Config.ini")
PARAINFO = get_param_info(CONFIG_INFO_FILE)


# 引入mysql操作函数
mysqlExe = torndb.Connection(
    host = "{0}:{1}".format(PARAINFO["DB_HOST"], PARAINFO["DB_PORT"]),
    database = PARAINFO["DB_NAME"],
    user = PARAINFO["USER_NAME"],
    password = PARAINFO["USER_PWD"],
)

busiDate = time.strftime('%Y%m%d', time.localtime(time.time()))
pro = ts.pro_api(PARAINFO["TUSHARE_TOKEN"])
tp = Tushare_Proc(pro, mysqlExe)

def imp_basic_datas_2_db():
    ''' 该函数集合执行的是无参数或固定参数的数据导入 '''
    tp.proc_main_stock_basic_datas()
    tp.proc_main_trade_cal_datas("20010101", "20181231")
    tp.proc_main_stock_company_datas()
    tp.proc_main_hs_const_datas("SH")
    tp.proc_main_hs_const_datas("SZ")
    tp.proc_main_new_share_datas()
    tp.proc_main_fund_company_datas()
    tp.proc_main_concept_datas({"inputCode": "ts", "codeType": "src"})
    tp.proc_main_fund_basic_datas({"inputCode": "E", "codeType": "market"})
    tp.proc_main_fund_basic_datas({"inputCode": "O", "codeType": "market"})

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

def imp_by_trade_date_datas_2_db(begDate, endDate):
    tradeDateList = tp.get_datas_for_db_trade_cal(begDate, endDate)
    print(tradeDateList)
    for x in tradeDateList:
        tradeDate = x["cal_date"]
        tp.proc_main_moneyflow_hsgt_datas({"inputCode": "{0}".format(tradeDate), "codeType": "start_date"})
        tp.proc_main_margin_detail_datas({"inputCode": "{0}".format(tradeDate), "codeType": "trade_date"})
        tp.proc_main_top_list_datas({"inputCode": "{0}".format(tradeDate), "codeType": "trade_date"})
        tp.proc_main_top_inst_datas({"inputCode": "{0}".format(tradeDate), "codeType": "trade_date"})
        tp.proc_main_repurchase_datas({"inputCode": "{0}".format(tradeDate), "codeType": "ann_date"})
        tp.proc_main_index_weight_datas({"inputCode": "{0}".format(tradeDate), "codeType": "trade_date"})
        tp.proc_main_fund_nav_datas({"inputCode": "{0}".format(tradeDate), "codeType": "end_date"})
        tp.proc_main_fund_div_datas({"inputCode": "{0}".format(tradeDate), "codeType": "ann_date"})
        tp.proc_main_fund_daily_datas({"inputCode": "{0}".format(tradeDate), "codeType": "trade_date"})
        tp.proc_main_bo_monthly_datas({"inputCode": "{0}".format(tradeDate), "codeType": "date"})
        tp.proc_main_bo_weekly_datas({"inputCode": "{0}".format(tradeDate), "codeType": "date"})
        tp.proc_main_bo_daily_datas({"inputCode": "{0}".format(tradeDate), "codeType": "date"})
        tp.proc_main_bo_cinema_datas({"inputCode": "{0}".format(tradeDate), "codeType": "date"})


def main():

    print(tp.get_table_field_list("hsgt_top10"))

    # imp_basic_datas_2_db()
    # imp_by_stock_code_datas_2_db()
    # imp_by_trade_date_datas_2_db("20180101", "20180107")




    rtnDatas = tp.get_datas_for_db_concept()
    for data in rtnDatas:
        tp.proc_main_concept_detail_datas({"inputCode":data["code"],"codeType":"id"})

    rtnDatas = tp.get_datas_for_db_sys_dict("market")
    for data in rtnDatas:
        tp.proc_main_index_basic_datas({"inputCode":data["dict_item"],"codeType":"market"})


    rtnDatas = tp.get_datas_for_db_sys_dict("item")
    for data in rtnDatas:
        tp.proc_main_tmt_twincome_datas({"inputCode":data["dict_item"], "codeType": "item"})
        tp.proc_main_tmt_twincomedetail_datas({"inputCode":data["dict_item"], "codeType": "item"})

    # 需要送ts_code入参 待完成双入参
    rtnDatas = tp.get_datas_for_db_index_basic()
    yyyyDict = [
        ["20100101","20101231"],
        ["20110101","20111231"],
        ["20120101","20121231"],
        ["20130101","20131231"],
        ["20140101","20141231"],
        ["20150101","20151231"],
        ["20160101","20161231"],
        ["20170101","20171231"],
        ["20180101","20181231"],
    ]
    for data in rtnDatas:
        for yyyy in yyyyDict :
            # print(yyyy)
            tp.proc_main_index_daily_datas({"inputCode": data["ts_code"], "codeType": "ts_code","startDate":yyyy[0], "endDate":yyyy[1]})



if __name__ == '__main__':
    main()