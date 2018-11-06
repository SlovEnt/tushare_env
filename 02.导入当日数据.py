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

def get_table_field_list(tableName):

    rtnDict = OrderedDict()

    strSql = """
        SELECT
            column_name
        FROM
            information_schema. COLUMNS
        WHERE
            0 = 0
        AND table_schema = 'tushare_datas'
        AND table_name = '%s'
    """ % tableName

    rtnDatas = mysqlExe.query(strSql)

    strFieldList = ""
    for x in rtnDatas:
        if x["column_name"] == "busi_date":
            continue

        if strFieldList == "":
            strFieldList = "%s" % (x["column_name"])
        else:
            strFieldList = "%s,%s" % (strFieldList, x["column_name"])

    rtnDict["StrFieldList"] = strFieldList

    return rtnDict

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

def get_stock_basic():

    tableName = "stock_basic"

    rtnDict = get_table_field_list(tableName)

    datas = pro.stock_basic(exchange='', list_status='', fields="%s" % rtnDict["StrFieldList"])

    dataDict = datas.to_dict("records")

    delete_date_row_2_mysql(tableName)
    insert_new_row_2_mysql(tableName, dataDict, "Y")

def get_trade_cal(begData, endData):

    tableName = "trade_cal"

    strSql = "delete from %s where cal_date between %s and %s;" % (tableName, begData, endData)
    mysqlExe.execute(strSql)

    datas = pro.trade_cal(exchange='', start_date=begData, end_date=endData)

    dataDict = datas.to_dict("records")
    insert_new_row_2_mysql(tableName, dataDict, "N")

def get_stock_company():

    tableName = "stock_company"
    datas = pro.stock_company(exchange='', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
    dataDict = datas.to_dict("records")

    delete_date_row_2_mysql(tableName)
    insert_new_row_2_mysql(tableName, dataDict, "Y")

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

def proc_main_daily_datas(trdDate):
    '''
    日线行情
    接口：daily
    更新时间：交易日每天15点～16点之间
    描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
    '''
    tableName = "daily"
    df = pro.daily(trade_date=trdDate)
    df = df.fillna(value=0)
    datas = df.to_dict("records")

    rtnMsg = tp.select_collect_flag(tableName, 'trade_date', trdDate)

    if len(datas) != 0 and rtnMsg is True:

        dataType = tp.get_table_column_data_type(tableName)
        try:
            # strSql = "delete from %s where trade_date = '%s';" % (tableName, trdDate)
            # mysqlExe.execute(strSql)

            tp.insert_new_datas_2_db(tableName, datas, dataType, "N")
            tp.insert_collect_flag(tableName, 'trade_date', trdDate)
        except Exception as e:
            traceback.print_exc()
    else:
        print("接口名称：{0}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName))

def main():

    rtnDateList = tp.get_datas_for_db_trade_cal(20180101, busiDate)

    if rtnDateList is not False:

        for rtnDate in rtnDateList:
            # proc_main_daily_datas(rtnDate["cal_date"])
            proc_main_daily_datas(rtnDate["cal_date"])




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