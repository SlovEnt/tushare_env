# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/4 22:38'

from collections import OrderedDict
import os
import re
import time
import tushare as ts

from chpackage.param_info import get_param_info
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_INFO_FILE = "%s/%s" % (BASE_DIR, "Config.ini")
PARAINFO = get_param_info(CONFIG_INFO_FILE)

from chpackage import torndb

# 引入mysql操作函数
mysqlExe = torndb.Connection(
    host = "{0}:{1}".format(PARAINFO["DB_HOST"], PARAINFO["DB_PORT"]),
    database = PARAINFO["DB_NAME"],
    user = PARAINFO["USER_NAME"],
    password = PARAINFO["USER_PWD"],
)

pro = ts.pro_api(PARAINFO["TUSHARE_TOKEN"])
busiDate = time.strftime('%Y%m%d', time.localtime(time.time()))

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


def insert_new_row_2_mysql(dataDict, tableName):

    for rowItem in dataDict:

        strSqlRowFeld = ""
        strSqlRowValue = ""
        for k,v in rowItem.items():
            if strSqlRowFeld == "":
                strSqlRowFeld = "`busi_date`, `{0}`".format(k)
            else:
                strSqlRowFeld = "{0}, `{1}`".format(strSqlRowFeld, k)

            if strSqlRowValue == "":
                strSqlRowValue = "'{0}','{1}'".format(busiDate, v)
            else:
                if v is None:
                    v = ""
                if "'" in str(v):
                    v = v.replace("'", "''")
                strSqlRowValue = "{0}, '{1}'".format(strSqlRowValue, v)

        strSql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tableName, strSqlRowFeld, strSqlRowValue)

        print(strSql)
        mysqlExe.execute(strSql)

def get_stock_basic(tableName):

    rtnDict = get_table_field_list(tableName)

    datas = pro.stock_basic(exchange='', list_status='', fields="%s" % rtnDict["StrFieldList"])

    dataDict = datas.to_dict("records")

    insert_new_row_2_mysql(dataDict, tableName)


def main():
    tableName = "stock_basic"
    get_stock_basic(tableName)

    # tableName = "trade_cal"
    # get_stock_basic(tableName)


if __name__ == '__main__':
    main()