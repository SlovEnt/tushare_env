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

def insert_new_row_2_mysql(tableName, dataDict, busiDateFlag):
    '''
    :param tableName:
    :param dataDict:
    :param busiDateFlag: N 表字段中无busi_date字段，Y 存在
    :return:
    '''

    i = 0
    for rowItem in dataDict:
        i += 1
        # insert sql
        strSqlRowFeld = ""
        strSqlRowValue = ""

        for k,v in rowItem.items():
            if strSqlRowFeld == "" and busiDateFlag == "Y":
                strSqlRowFeld = "`busi_date`, `{0}`".format(k)
            elif strSqlRowFeld == "" and busiDateFlag == "N":
                strSqlRowFeld = "`{0}`".format(k)
            else:
                strSqlRowFeld = "{0}, `{1}`".format(strSqlRowFeld, k)

            if strSqlRowValue == ""  and busiDateFlag == "Y":
                strSqlRowValue = "'{0}','{1}'".format(busiDate, v)
            elif strSqlRowValue == ""  and busiDateFlag == "N":
                strSqlRowValue = "'{0}'".format(v)
            else:
                ''' 特殊处理 转换类型 '''
                if k == "volume_ratio":
                    v = 0
                if v is None:
                    v = ""
                if "'" in str(v):
                    v = v.replace("'", "''")
                if "%" in str(v):
                    v = v.replace("%", "%%")
                strSqlRowValue = "{0}, '{1}'".format(strSqlRowValue, v)

        strSql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tableName, strSqlRowFeld, strSqlRowValue)

        print(i, strSql)

        mysqlExe.execute(strSql)

def delete_date_row_2_mysql(tableName):

    strSql = "delete from %s where busi_date = '%s'" % (tableName, busiDate)
    mysqlExe.execute(strSql)


def get_hs_const(hsType):
    tableName = "hs_const"
    datas = pro.hs_const(hs_type=hsType)
    dataDict = datas.to_dict("records")
    delete_date_row_2_mysql(tableName)
    insert_new_row_2_mysql(tableName, dataDict, "Y")

def get_namechange():
    tableName = "namechange"
    strSql = "delete from %s;" % (tableName)
    mysqlExe.execute(strSql)
    datas = pro.namechange(ts_code='', fields='ts_code,name,start_date,end_date,ann_date,change_reason')
    dataDict = datas.to_dict("records")
    insert_new_row_2_mysql(tableName, dataDict, "N")

def get_new_share(begData, endData):
    tableName = "new_share"
    strSql = "delete from %s where ipo_date between %s and %s;" % (tableName, begData, endData)
    mysqlExe.execute(strSql)
    datas = pro.new_share(start_date=begData, end_date=endData)
    dataDict = datas.to_dict("records")
    insert_new_row_2_mysql(tableName, dataDict, "N")

def get_daily(tradeDate):
    tableName = "daily"
    strSql = "delete from %s where trade_date = %s;" % (tableName, tradeDate)
    mysqlExe.execute(strSql)

    datas = pro.daily(trade_date=tradeDate)
    dataDict = datas.to_dict("records")
    insert_new_row_2_mysql(tableName, dataDict, "N")

def get_adj_factor(tradeDate):
    tableName = "adj_factor"
    strSql = "delete from %s where trade_date = %s;" % (tableName, tradeDate)
    mysqlExe.execute(strSql)

    datas = pro.adj_factor(ts_code='', trade_date=tradeDate)
    dataDict = datas.to_dict("records")
    insert_new_row_2_mysql(tableName, dataDict, "N")

def get_suspend(tsCode):
    try:
        tableName = "suspend"
        strSql = "delete from %s where ts_code = '%s';" % (tableName, tsCode)
        mysqlExe.execute(strSql)

        datas = pro.suspend(ts_code='%s' % tsCode, suspend_date='', resume_date='', fiedls='')
        dataDict = datas.to_dict("records")
        insert_new_row_2_mysql(tableName, dataDict, "N")
    except Exception as e:
        print(e)

def get_daily_basic(tsCode):
    try:
        tableName = "daily_basic"
        strSql = "delete from %s where ts_code = '%s';" % (tableName, tsCode)
        mysqlExe.execute(strSql)

        datas = pro.daily_basic(ts_code='%s' % tsCode, trade_date='')
        dataDict = datas.to_dict("records")

        insert_new_row_2_mysql(tableName, dataDict, "N")
    except Exception as e:
        print(e)

def get_income():
    try:
        tableName = "income"
        tsCodeList = mysqlExe.query(
            "select ts_code from stock_basic where ts_code not in (select key_detail from collect_flag where flag = 'Y' and func_name='{0}' and key_word='{1}') ;".format(
                tableName, "ts_code"))

        for tsCode in tsCodeList:
            tsCode = tsCode["ts_code"]
            strSql = "delete from {0} where ts_code = '{1}'".format(tableName, tsCode)
            mysqlExe.execute(strSql)
            datas = tp.get_datas_for_ts_income(tsCode)
            if datas is not False:
                dataType = tp.get_table_column_data_type(tableName)
                tp.insert_new_datas_2_db(tableName, datas, dataType, "N")
                tp.insert_collect_flag(tableName, 'ts_code', tsCode, 'Y')
    except Exception as e:
        print(e)





def main():

    print(tp.get_table_field_list("fina_audit"))

    # tp.proc_main_stock_basic_datas()
    # tp.proc_main_trade_cal_datas("20180101", "20181231")
    # tp.proc_main_stock_company_datas()
    # tp.proc_main_hs_const_datas("SH")
    # tp.proc_main_hs_const_datas("SZ")
    # tp.proc_main_new_share_datas()


    pool = Pool(10)
    stockCodeList = tp.get_datas_for_db_stock_basic()
    if stockCodeList is not False:
        for stockCode in stockCodeList:

            argsDict = {}
            argsDict["inputCode"] = stockCode["ts_code"]
            argsDict["codeType"] = "ts_code"

            # tp.proc_main_adj_factor_datas(argsDict)
            # pool.apply_async(tp.proc_main_adj_factor_datas, args=(argsDict,))

            # stockCode = argsDict["inputCode"]
            # tp.proc_main_suspend_datas(stockCode)

            # tp.proc_main_daily_basic_datas(argsDict)

            # tp.proc_main_income_datas(argsDict)

            # tp.proc_main_balancesheet_datas(argsDict)
            # tp.proc_main_cashflow_datas(argsDict)
            # tp.proc_main_forecast_datas(argsDict)
            # tp.proc_main_express_datas(argsDict)
            # tp.proc_main_dividend_datas(argsDict)
            # tp.proc_main_fina_indicator_datas(argsDict)
            tp.proc_main_fina_audit_datas(argsDict)





            # pool.apply_async(func=tp.proc_main_suspend_datas, args=(stockCode,))
    pool.close()
    pool.join()

    # rtnDateList = tp.get_datas_for_db_trade_cal(busiDate, busiDate)
    #
    # if rtnDateList is not False:
    #
    #     for rtnDate in rtnDateList:
    #         # proc_main_daily_datas(rtnDate["cal_date"])
    #         proc_main_daily_datas(rtnDate["cal_date"])




    # get_income()



    # pool = Pool(10)


        # pool.apply_async(func=tp.insert_new_datas_2_db, args=(tableName, datas, dataType, "N",))
    # pool.close()
    # pool.join()

    # print(get_table_field_list("namechange"))

    #####################################################################
    # get_stock_basic()

    # get_trade_cal("20060101", "20181231")

    # get_stock_company()

    # get_hs_const("SH")
    # get_hs_const("SZ")

    # get_namechange()

    # get_new_share(busiDate, busiDate)

    # pool = Pool(10)
    # tradeDays = mysqlExe.query("select cal_date from trade_cal where is_open=1 and cal_date between 20180101 and %s order by cal_date;" % busiDate)
    #
    #
    # for tradeDay in tradeDays:
    #     tradeDate = tradeDay["cal_date"]
    #     pool.apply_async(func=get_adj_factor, args=(tradeDate,))
    #     # get_daily(tradeDate)
    # pool.close()
    # pool.join()


    # pool = Pool(10)
    # tsCodeList = mysqlExe.query("select ts_code from stock_basic;")
    #
    # for tsCode in tsCodeList:
    #     tsCode = tsCode["ts_code"]
    #     pool.apply_async(func=get_suspend, args=(tsCode,))
    #     # get_daily(tradeDate)
    # pool.close()
    # pool.join()


    # pool = Pool(10)
    # tsCodeList = mysqlExe.query("select ts_code from stock_basic;")
    #
    # for tsCode in tsCodeList:
    #     tsCode = tsCode["ts_code"]
    #     pool.apply_async(func=get_daily_basic, args=(tsCode,))
    #     # get_daily(tradeDate)
    # pool.close()
    # pool.join()

    # pool = Pool(10)
    # tsCodeList = mysqlExe.query("select ts_code from stock_basic;")
    #
    # for tsCode in tsCodeList:
    #     tsCode = tsCode["ts_code"]
    #     pool.apply_async(func=get_income, args=(tsCode,))
    # pool.close()
    # pool.join()

#     desc = mysqlExe.query("""
#     select column_name,data_type
# from information_schema.columns
# where table_name='income'
#     """)
#     print(desc)

if __name__ == '__main__':
    main()