# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/5 21:42'

import time
import os
from collections import OrderedDict


class Tushare_Proc_v2(object):
    '''

    '''
    def __init__(self, pro, mysqlExe, busiDate=None):
        self.pro = pro
        self.mysqlExe = mysqlExe
        if busiDate is None:
            self.busiDate = time.strftime('%Y%m%d', time.localtime(time.time()))
        else:
            self.busiDate = busiDate

    def get_table_column_data_type(self, tableName):
        ''' 从数据库中返回字段类型 float类型的字段必须将None改为0 '''
        strSql = "select column_name,data_type from information_schema.columns where table_name='{0}'".format(tableName)
        try:
            rtnDataType = self.mysqlExe.query(strSql)
            return rtnDataType
        except:
            return False

    def get_table_field_list(self, tableName):

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

        rtnDatas = self.mysqlExe.query(strSql)

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

    def truncate_table(self,tableName):
        strSql = "truncate table {0};".format(tableName)
        self.mysqlExe.execute(strSql)

    def select_collect_flag(self, tableName, key_word, key_detail):
        strSql = "select count(*) as cnt from collect_flag a where a.func_name='%s' and a.key_word='%s' and a.key_detail='%s' and a.collect_date='%s'" % (
        tableName, key_word, key_detail, self.busiDate)
        rtnCnt = self.mysqlExe.query(strSql)
        if rtnCnt[0]["cnt"] == 0:
            return True
        else:
            return False

    def insert_collect_flag(self, tableName, key_word, key_detail):
        try:
            strSql = "insert into collect_flag(`func_name`, `key_word`, `key_detail`, `flag`, `collect_date`) VALUES ('%s', '%s', '%s', '%s', '%s')" % (
            tableName, key_word, key_detail, "Y", self.busiDate)
            self.mysqlExe.execute(strSql)
            return True
        except:
            return False

    def delete_collect_flag(self, tableName, key_word, key_detail):
        try:
            # strSql = "delete from {0} where {1} = '{2}'".format(tableName, key_word, key_detail)
            # print(strSql)
            # self.mysqlExe.execute(strSql)
            strSql = "delete from collect_flag where func_name = '{0}' and key_word = '{1}' and key_detail = '{2}' and collect_date = '{3}'".format(tableName, key_word, key_detail, self.busiDate)
            self.mysqlExe.execute(strSql)
            return True
        except:
            return False

    def insert_new_datas_2_db(self, tableName, datas, datasType, busiDateFlag):

        floatFileds = []

        for dt in datasType:
            if dt["data_type"] == "float" or dt["data_type"] == "int":
                floatFileds.append(dt["column_name"])

        for rowItem in datas:
            strSqlRowFeld = ""
            strSqlRowValue = ""
            for k, v in rowItem.items():
                if strSqlRowFeld == "" and busiDateFlag == "Y":
                    strSqlRowFeld = "`busi_date`, `{0}`".format(k)
                elif strSqlRowFeld == "" and busiDateFlag == "N":
                    strSqlRowFeld = "`{0}`".format(k)
                else:
                    strSqlRowFeld = "{0}, `{1}`".format(strSqlRowFeld, k)

                # if str(type(v)) == float and v >= 0:
                #     v = v
                # elif str(v) == "nan":
                #     v = 0
                if k in floatFileds and v is None:
                    v = 0
                elif v is None:
                    v = ""

                if strSqlRowValue == "" and busiDateFlag == "Y":
                    strSqlRowValue = "'{0}','{1}'".format(self.busiDate, v)
                elif strSqlRowValue == "" and busiDateFlag == "N":
                    strSqlRowValue = "'{0}'".format(v)
                else:
                    ''' 特殊字符处理 转换类型 '''
                    if "'" in str(v):
                        v = v.replace("'", "''")
                    if "%" in str(v):
                        v = v.replace("%", "%%")
                    strSqlRowValue = "{0}, '{1}'".format(strSqlRowValue, v)

            strSql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tableName, strSqlRowFeld, strSqlRowValue)

            print( strSql)
            try:
                self.mysqlExe.execute(strSql)
            except Exception as e:
                print(e)

    def get_insert_sql(self, tableName, datas, datasType, busiDateFlag):

        floatFileds = []

        strSqlList = []

        for dt in datasType:
            if dt["data_type"] == "float" or dt["data_type"] == "int":
                floatFileds.append(dt["column_name"])

        for rowItem in datas:
            strSqlRowFeld = ""
            strSqlRowValue = ""
            for k, v in rowItem.items():
                if strSqlRowFeld == "" and busiDateFlag == "Y":
                    strSqlRowFeld = "`busi_date`, `{0}`".format(k)
                elif strSqlRowFeld == "" and busiDateFlag == "N":
                    strSqlRowFeld = "`{0}`".format(k)
                else:
                    strSqlRowFeld = "{0}, `{1}`".format(strSqlRowFeld, k)

                # if str(type(v)) == float and v >= 0:
                #     v = v
                # elif str(v) == "nan":
                #     v = 0
                if k in floatFileds and v is None:
                    v = 0
                elif v is None:
                    v = ""

                if strSqlRowValue == "" and busiDateFlag == "Y":
                    strSqlRowValue = "'{0}','{1}'".format(self.busiDate, v)
                elif strSqlRowValue == "" and busiDateFlag == "N":
                    strSqlRowValue = "'{0}'".format(v)
                else:
                    ''' 特殊字符处理 转换类型 '''
                    if "'" in str(v):
                        v = v.replace("'", "''")
                    if "%" in str(v):
                        v = v.replace("%", "%%")
                    strSqlRowValue = "{0}, '{1}'".format(strSqlRowValue, v)

            strSql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tableName, strSqlRowFeld, strSqlRowValue)

            strSqlList.append(strSql)

        return strSqlList
            # try:
            #     self.mysqlExe.execute(strSql)
            # except Exception as e:
            #     print(e)

    def get_datas_for_db_trade_cal(self, begDate, endDate):
        try:
            strSql = "select cal_date from trade_cal a where 0=0 and a.exchange in ('SSE','SZSE') and is_open=1 and cal_date between {0} and {1} order by cal_date".format(begDate, endDate)
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_concept(self):
        try:
            strSql = "select code from concept a where 0=0 order by code;"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_sys_dict(self, dictId, dictItem='*'):
        try:
            if dictItem == "*":
                strSql = "select dict_item from sys_dict a where 0=0 and dict_id = '{0}' order by dict_item;".format(dictId)
            else:
                strSql = "select dict_item from sys_dict a where 0=0 and dict_id = '{0}' and dict_item = '{1}' order by dict_item;".format(dictId, dictItem)
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_stock_basic(self):
        try:
            strSql = "select ts_code from stock_basic order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_ts_income(self, tsCode):

        # 定时器
        timerCount = 60
        callCnt = 80
        interval = callCnt / timerCount

        try:
            tableName = "income"
            df = self.pro.income(ts_code='{0}'.format(tsCode), start_date='', end_date='')
            datas = df.to_dict("records")
            return datas
        except:
            return False

    def get_datas_for_ts_daily(self, trdDate):
        '''
        日线行情
        接口：daily
        更新时间：交易日每天15点～16点之间
        描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
        '''
        try:
            tableName = "daily"
            # strSql = "delete from %s where trade_date = %s;" % (tableName, trdDate)
            # self.mysqlExe.execute(strSql)
            df = self.pro.daily(trade_date=trdDate)
            datas = df.to_dict("records")
            return datas
        except:
            return False

    def get_datas_for_db_index_basic(self):
        try:
            strSql = "select ts_code from index_basic order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    #################################################################

    def get_insertsql_stock_basic(self, argsDict):
        '''
        股票列表
        接口：stock_basic
        描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        '''
        # print(tp.get_table_field_list("stock_basic"))
        tableName = "stock_basic"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            df = self.pro.stock_basic(exchange='', list_status='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            df = df.fillna(value=0)
            datas = df.to_dict("records")
            dataType = []
            try:
                self.truncate_table(tableName)
                strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)

            return strSqlList

        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))


    def get_tpdatas_trade_cal(self,):
        '''
        股票列表
        接口：trade_cal
        描述：获取各大交易所交易日历数据,默认提取的是上交所
        '''
        tableName = "trade_cal"

        # 判断当前日期是否大于YYYY1220日 如果大于，则开始获取下一年度交易日历
        yyyy = time.strftime("%Y")
        mmdd = time.strftime('%m%d')

        yyyyNext = int(yyyy) + 1

        if int(mmdd) > 1220:
            startDate = "%s1201" % yyyy # 为防止当年最后几天被调整为节假日 需要多取一个月的日期
            endDate = "%s1231" % yyyyNext
        else:
            print("接口名称：{0} ，不在指定采集时间范围内(>{1}1220)，无需再次采集！".format(tableName, yyyy))
            return ""

        df = self.pro.trade_cal(exchange='', start_date=startDate, end_date=endDate)
        # df = self.pro.trade_cal(exchange='', start_date="20180101", end_date="20191231")
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            strSql = "delete from trade_cal where cal_date between {0} and {1}".format(startDate, endDate)
            # strSql = "delete from trade_cal where cal_date between {0} and {1}".format("20180101", "20191231")
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)


class Tushare_Proc(object):

    def __init__(self, pro, mysqlExe, busiDate=None):
        self.pro = pro
        self.mysqlExe = mysqlExe
        if busiDate is None:
            self.busiDate = time.strftime('%Y%m%d', time.localtime(time.time()))
        else:
            self.busiDate = busiDate

    def get_table_column_data_type(self, tableName):
        ''' 从数据库中返回字段类型 float类型的字段必须将None改为0 '''
        strSql = "select column_name,data_type from information_schema.columns where table_name='{0}'".format(tableName)
        try:
            rtnDataType = self.mysqlExe.query(strSql)
            return rtnDataType
        except:
            return False

    def get_table_field_list(self, tableName):

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

        rtnDatas = self.mysqlExe.query(strSql)

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

    def truncate_table(self,tableName):
        strSql = "truncate table {0};".format(tableName)
        self.mysqlExe.execute(strSql)

    def select_collect_flag(self, tableName, key_word, key_detail):
        strSql = "select count(*) as cnt from collect_flag a where a.func_name='%s' and a.key_word='%s' and a.key_detail='%s' and a.collect_date='%s'" % (
        tableName, key_word, key_detail, self.busiDate)
        rtnCnt = self.mysqlExe.query(strSql)
        if rtnCnt[0]["cnt"] == 0:
            return True
        else:
            return False

    def insert_collect_flag(self, tableName, key_word, key_detail):
        try:
            strSql = "insert into collect_flag(`func_name`, `key_word`, `key_detail`, `flag`, `collect_date`) VALUES ('%s', '%s', '%s', '%s', '%s')" % (
            tableName, key_word, key_detail, "Y", self.busiDate)
            self.mysqlExe.execute(strSql)
            return True
        except:
            return False

    def delete_collect_flag(self, tableName, key_word, key_detail):
        try:
            # strSql = "delete from {0} where {1} = '{2}'".format(tableName, key_word, key_detail)
            # print(strSql)
            # self.mysqlExe.execute(strSql)
            strSql = "delete from collect_flag where func_name = '{0}' and key_word = '{1}' and key_detail = '{2}' and collect_date = '{3}'".format(tableName, key_word, key_detail, self.busiDate)
            self.mysqlExe.execute(strSql)
            return True
        except:
            return False

    def insert_new_datas_2_db(self, tableName, datas, datasType, busiDateFlag):

        floatFileds = []

        for dt in datasType:
            if dt["data_type"] == "float" or dt["data_type"] == "int":
                floatFileds.append(dt["column_name"])

        for rowItem in datas:
            strSqlRowFeld = ""
            strSqlRowValue = ""
            for k, v in rowItem.items():
                if strSqlRowFeld == "" and busiDateFlag == "Y":
                    strSqlRowFeld = "`busi_date`, `{0}`".format(k)
                elif strSqlRowFeld == "" and busiDateFlag == "N":
                    strSqlRowFeld = "`{0}`".format(k)
                else:
                    strSqlRowFeld = "{0}, `{1}`".format(strSqlRowFeld, k)

                # if str(type(v)) == float and v >= 0:
                #     v = v
                # elif str(v) == "nan":
                #     v = 0
                if k in floatFileds and v is None:
                    v = 0
                elif v is None:
                    v = ""

                if strSqlRowValue == "" and busiDateFlag == "Y":
                    strSqlRowValue = "'{0}','{1}'".format(self.busiDate, v)
                elif strSqlRowValue == "" and busiDateFlag == "N":
                    strSqlRowValue = "'{0}'".format(v)
                else:
                    ''' 特殊字符处理 转换类型 '''
                    if "'" in str(v):
                        v = v.replace("'", "''")
                    if "%" in str(v):
                        v = v.replace("%", "%%")
                    strSqlRowValue = "{0}, '{1}'".format(strSqlRowValue, v)

            strSql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tableName, strSqlRowFeld, strSqlRowValue)

            print( strSql)
            try:
                self.mysqlExe.execute(strSql)
            except Exception as e:
                print(e)

    def get_datas_for_db_trade_cal(self, begDate, endDate):
        try:
            strSql = "select cal_date from trade_cal a where 0=0 and a.exchange in ('SSE','SZSE') and is_open=1 and cal_date between {0} and {1} order by cal_date".format(begDate, endDate)
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_concept(self):
        try:
            strSql = "select code from concept a where 0=0 order by code;"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_sys_dict(self, dictId, dictItem='*'):
        try:
            if dictItem == "*":
                strSql = "select dict_item from sys_dict a where 0=0 and dict_id = '{0}' order by dict_item;".format(dictId)
            else:
                strSql = "select dict_item from sys_dict a where 0=0 and dict_id = '{0}' and dict_item = '{1}' order by dict_item;".format(dictId, dictItem)
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_stock_basic(self):
        try:
            strSql = "select ts_code from stock_basic order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_ts_income(self, tsCode):

        # 定时器
        timerCount = 60
        callCnt = 80
        interval = callCnt / timerCount

        try:
            tableName = "income"
            df = self.pro.income(ts_code='{0}'.format(tsCode), start_date='', end_date='')
            datas = df.to_dict("records")
            return datas
        except:
            return False

    def get_datas_for_ts_daily(self, trdDate):
        '''
        日线行情
        接口：daily
        更新时间：交易日每天15点～16点之间
        描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
        '''
        try:
            tableName = "daily"
            # strSql = "delete from %s where trade_date = %s;" % (tableName, trdDate)
            # self.mysqlExe.execute(strSql)
            df = self.pro.daily(trade_date=trdDate)
            datas = df.to_dict("records")
            return datas
        except:
            return False

    def get_datas_for_db_index_basic(self):
        try:
            strSql = "select ts_code from index_basic order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False



    #################################################################


    def proc_main_stock_basic_datas(self, argsDict):
        '''
        股票列表
        接口：stock_basic
        描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        '''
        # print(tp.get_table_field_list("stock_basic"))
        tableName = "stock_basic"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            df = self.pro.stock_basic(exchange='', list_status='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            df = df.fillna(value=0)
            datas = df.to_dict("records")
            dataType = []
            try:
                self.truncate_table(tableName)
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)

        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_stock_company_datas(self, argsDict):
        '''
        上市公司基本信息
        接口：stock_company
        描述：获取上市公司基础信息
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "stock_company"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:
            df = self.pro.stock_company(exchange='{0}'.format(inputCode), fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,'
                                                             'main_business,business_scope')
            df = df.fillna(value=0)
            datas = df.to_dict("records")
            dataType = self.get_table_column_data_type(tableName)
            try:
                # self.truncate_table(tableName)
                strSql = "delete from stock_company where exchange = '{0}'".format(inputCode)
                # strSql = "delete from trade_cal where cal_date between {0} and {1}".format("20180101", "20191231")
                self.mysqlExe.execute(strSql)
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)

        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_trade_cal_datas(self,):
        '''
        股票列表
        接口：trade_cal
        描述：获取各大交易所交易日历数据,默认提取的是上交所
        '''
        tableName = "trade_cal"

        # 判断当前日期是否大于YYYY1220日 如果大于，则开始获取下一年度交易日历
        yyyy = time.strftime("%Y")

        mmdd = time.strftime('%m%d')

        yyyyNext = int(yyyy) + 1

        if int(mmdd) > 1220:
            startDate = "%s1201" % yyyy # 为防止当年最后几天被调整为节假日 需要多取一个月的日期
            endDate = "%s1231" % yyyyNext
        else:
            print("接口名称：{0} ，不在指定采集时间范围内(>{1}1220)，无需再次采集！".format(tableName, yyyy))
            return ""

        df = self.pro.trade_cal(exchange='', start_date=startDate, end_date=endDate)
        # df = self.pro.trade_cal(exchange='', start_date="20180101", end_date="20191231")
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            strSql = "delete from trade_cal where cal_date between {0} and {1}".format(startDate, endDate)
            # strSql = "delete from trade_cal where cal_date between {0} and {1}".format("20180101", "20191231")
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_namechange_datas(self, argsDict):
        '''
        股票曾用名
        接口：namechange
        描述：历史名称变更记录
        '''
        tableName = "namechange"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            df = self.pro.namechange()
            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.truncate_table(tableName)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_hs_const_datas(self, argsDict):
        '''
        沪深股通成份股
        接口：hs_const
        描述：获取沪股通、深股通成分数据
        '''
        tableName = "hs_const"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:
            df = self.pro.hs_const(hs_type=inputCode)
            df = df.fillna(value=0)
            datas = df.to_dict("records")
            dataType = []
            try:
                strSql = "delete from hs_const where hs_type = '{0}'".format(inputCode)
                self.mysqlExe.execute(strSql)
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)

        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_new_share_datas(self, argsDict):
        '''
        接口：new_share
        描述：获取新股上市列表数据
        限量：单次最大2000条，总量不限制
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''

        tableName = "new_share"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            df = self.pro.new_share()
            df = df.fillna(value=0)
            datas = df.to_dict("records")
            dataType = []
            try:
                self.truncate_table(tableName)
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)

        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_daily_datas(self, argsDict):
        '''
        日线行情
        接口：daily
        更新时间：交易日每天15点～16点之间
        描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
        '''
        tableName = "daily"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]
        startDate = argsDict["start_date"]
        endDate = argsDict["end_date"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.daily(ts_code=inputCode)
            elif codeType == "trade_date":
                df = self.pro.daily(trade_date=inputCode)
            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_adj_factor_datas(self, argsDict):
        '''
        复权因子
        接口：adj_factor
        更新时间：早上9点30分
        描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
        '''
        tableName = "adj_factor"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.adj_factor(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.adj_factor(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_suspend_datas(self, argsDict):
        '''
        停复牌信息
        接口：suspend
        更新时间：不定期
        描述：获取股票每日停复牌信息
        '''
        tableName = "suspend"
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "suspend_date":
            df = self.pro.suspend(ts_code='', suspend_date='{0}'.format(inputCode), resume_date='', fiedls='ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type')
        elif codeType == "resume_date":
            df = self.pro.suspend(ts_code='', suspend_date='', resume_date='{0}'.format(inputCode), fiedls='ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type')
        else:
            df = self.pro.suspend(ts_code='{0}'.format(inputCode), suspend_date='', resume_date='', fiedls='ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type')
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_daily_basic_datas(self, argsDict):
        '''
        每日指标
        接口：daily_basic
        更新时间：交易日每日15点～17点之间
        描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
        '''
        tableName = "daily_basic"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.daily_basic(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.daily_basic(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_income_datas(self, argsDict):
        '''
        利润表
        接口：income
        描述：获取上市公司财务利润表数据
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "income"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.income(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.income(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_balancesheet_datas(self, argsDict):
        '''
        资产负债表
        接口：balancesheet
        描述：获取上市公司资产负债表
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "balancesheet"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.balancesheet(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.balancesheet(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_cashflow_datas(self, argsDict):
        '''
        现金流量表
        接口：cashflow
        描述：获取上市公司现金流量表
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "cashflow"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.cashflow(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.cashflow(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_forecast_datas(self, argsDict):
        '''
        现金流量表
        接口：cashflow
        描述：获取上市公司现金流量表
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "forecast"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.forecast(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.forecast(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_express_datas(self, argsDict):
        '''
        业绩快报
        接口：express
        描述：获取上市公司业绩快报
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "express"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.express(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.express(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_dividend_datas(self, argsDict):
        '''
        分红送股
        接口：dividend
        描述：分红送股数据
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "dividend"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.dividend(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.dividend(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_fina_indicator_datas(self, argsDict):
        '''
        财务指标数据
        接口：fina_indicator
        描述：获取上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回30条记录，可通过设置日期多次请求获取更多数据。
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_indicator"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.fina_indicator(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.fina_indicator(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_fina_audit_datas(self, argsDict):
        '''
        财务审计意见
        接口：fina_audit
        描述：获取上市公司定期财务审计意见数据
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_audit"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.fina_audit(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.fina_audit(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_fina_mainbz_datas(self, argsDict):
        '''
        主营业务构成
        接口：fina_mainbz
        描述：获得上市公司主营业务构成，分地区和产品两种方式
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_mainbz"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.fina_mainbz(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.fina_mainbz(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_moneyflow_hsgt_datas(self, argsDict):
        '''
        接口：moneyflow_hsgt
        描述：获取沪股通、深股通、港股通每日资金流向数据
        '''
        tableName = "moneyflow_hsgt"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "start_date":
                df = self.pro.moneyflow_hsgt(start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.moneyflow_hsgt(trade_date ='{0}'.format(inputCode))

            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_hsgt_top10_datas(self, argsDict):
        '''
        沪深股通十大成交股
        接口：hsgt_top10
        描述：获取沪股通、深股通每日前十大成交详细数据
        '''
        tableName = "hsgt_top10"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.hsgt_top10(ts_code='{0}'.format(inputCode))
        elif codeType == "trade_date":
            df = self.pro.hsgt_top10(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_ggt_top10_datas(self, argsDict):
        '''
        港股通十大成交股
        接口：ggt_top10
        描述：获取港股通每日成交数据，其中包括沪市、深市详细数据
        '''
        tableName = "ggt_top10"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.ggt_top10(ts_code='{0}'.format(inputCode))
        elif codeType == "trade_date":
            df = self.pro.ggt_top10(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_margin_detail_datas(self, argsDict):
        '''
        融资融券交易明细
        接口：margin_detail
        描述：获取沪深两市每日融资融券明细
        '''
        tableName = "margin_detail"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.margin_detail(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.margin_detail(trade_date='{0}'.format(inputCode))

            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_margin_datas(self, argsDict):
        '''
        融资融券交易汇总
        接口：margin
        描述：获取融资融券每日交易汇总数据
        '''
        tableName = "margin"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.margin_detail(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.margin(trade_date='{0}'.format(inputCode))

            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_top10_holders_datas(self, argsDict):
        '''
        前十大股东
        接口：top10_holders
        描述：获取上市公司前十大股东数据，包括持有数量和比例等信息。
        '''
        tableName = "top10_holders"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.top10_holders(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.top10_holders(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_top10_floatholders_datas(self, argsDict):
        '''
        前十大流通股东
        接口：top10_floatholders
        描述：获取上市公司前十大流通股东数据。
        '''
        tableName = "top10_floatholders"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.top10_floatholders(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.top10_floatholders(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_top_list_datas(self, argsDict):
        '''
        龙虎榜每日明细
        接口：top_list
        描述：龙虎榜每日交易明细
        数据历史： 2005年至今
        限量：单次最大10000
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "top_list"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.top_list(ts_code='{0}'.format(inputCode), trade_date='{0}'.format(self.busiDate))
            elif codeType == "trade_date":
                df = self.pro.top_list(trade_date='{0}'.format(inputCode))

            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_top_inst_datas(self, argsDict):
        '''
        龙虎榜机构明细
        接口：top_inst
        描述：龙虎榜机构成交明细
        限量：单次最大10000
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "top_inst"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.top_inst(ts_code='{0}'.format(inputCode), trade_date='{0}'.format(self.busiDate))
            elif codeType == "trade_date":
                df = self.pro.top_inst(trade_date='{0}'.format(inputCode))

            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_pledge_stat_datas(self, argsDict):
        '''
        股权质押统计数据
        接口：pledge_stat
        描述：获取股权质押统计数据
        限量：单次最大1000
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "pledge_stat"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.pledge_stat(ts_code='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_share_float_datas(self, argsDict):
        '''
        接口：share_float
        描述：获取限售股解禁
        限量：单次最大5000条，总量不限制
        积分：120分可调取，每分钟内限制次数，超过5000积分无限制，具体请参阅积分获取办法
        '''
        tableName = "share_float"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.share_float(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.share_float(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_block_trade_datas(self, argsDict):
        '''
        接口：block_trade
        描述：大宗交易
        限量：单次最大1000条，总量不限制
        积分：300积分可调取，每分钟内限制次数，超过5000积分无限制，具体请参阅积分获取办法
        '''
        tableName = "block_trade"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.block_trade(ts_code='{0}'.format(inputCode))
        elif codeType == "ann_date":
            df = self.pro.block_trade(ts_code='', ann_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_pledge_detail_datas(self, argsDict):
        '''
        股权质押明细
        接口：pledge_detail
        描述：获取股权质押明细数据
        限量：单次最大1000
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "pledge_detail"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.pledge_detail(ts_code='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_repurchase_datas(self, argsDict):
        '''
        股票回购
        接口：repurchase
        描述：获取上市公司回购股票数据
        积分：用户需要至少600积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "repurchase"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.repurchase(ann_date='{0}'.format(inputCode))
            elif codeType == "ann_date":
                df = self.pro.repurchase(ann_date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_concept_datas(self, argsDict):
        '''
        概念股分类
        接口：concept
        描述：获取概念股分类，目前只有ts一个来源，未来将逐步增加来源
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "concept"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "src":
                df = self.pro.concept(src='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, 0, 0)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_concept_detail_datas(self, argsDict):
        '''
        概念股列表
        接口：concept_detail
        描述：获取概念股分类明细数据
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "concept_detail"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "id":
                df = self.pro.concept_detail(id='{0}'.format(inputCode), fields='id, ts_code, name, in_date, out_date')
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from concept_detail where id = '{0}';".format(inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_index_basic_datas(self, argsDict):
        '''
        指数基本信息
        接口：index_basic
        描述：获取指数基础信息。
        '''
        tableName = "index_basic"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "market":
                df = self.pro.index_basic(market='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_index_daily_datas(self, argsDict):
        '''
        指数日线行情
        接口：index_daily
        描述：获取指数每日行情，还可以通过bar接口获取。由于服务器压力，目前规则是单次调取最多取2800行记录，可以设置start和end日期补全。指数行情也可以通过通用行情接口获取数据．
        '''
        tableName = "index_daily"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]
        # startDate = argsDict["startDate"]
        # endDate = argsDict["endDate"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.index_daily(ts_code='{0}'.format(inputCode), start_date='', end_date='')
        elif codeType == "trade_date":
            df = self.pro.index_daily(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_index_weight_datas(self, argsDict):
        '''
        指数成分和权重
        接口：index_weight
        描述：获取各类指数成分和权重，月度数据 ，如需日度指数成分和权重，请联系 waditu@163.com
        来源：指数公司网站公开数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "index_weight"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "index_code":
                df = self.pro.index_weight(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.index_weight(trade_date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_fund_basic_datas(self, argsDict):
        '''
        公募基金列表
        接口：fund_basic
        描述：获取公募基金数据列表，包括场内和场外基金
        积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
        交易市场: E场内 O场外（默认E）
        '''
        tableName = "fund_basic"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "market":
                '''交易市场: E场内 O场外（默认E）'''
                df = self.pro.fund_basic(market='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_fund_company_datas(self, argsDict):
        '''
        公募基金公司
        接口：fund_company
        描述：获取公募基金管理人列表
        积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_company"

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            df = self.pro.fund_company()

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    self.truncate_table(tableName)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_fund_nav_datas(self, argsDict):
        '''
        公募基金净值
        接口：fund_nav
        描述：获取公募基金净值数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_nav"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.fund_nav(ts_code='{0}'.format(inputCode))
            elif codeType == "end_date":
                df = self.pro.fund_nav(end_date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_fund_div_datas(self, argsDict):
        '''
        公募基金分红
        接口：fund_div
        描述：获取公募基金分红数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_div"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ann_date":
                df = self.pro.fund_div(ann_date='{0}'.format(inputCode))
            elif codeType == "ex_date":
                df = self.pro.fund_div(ex_date='{0}'.format(inputCode))
            elif codeType == "ex_date":
                df = self.pro.fund_div(pay_date='{0}'.format(inputCode))
            elif codeType == "ex_date":
                df = self.pro.fund_div(ts_code='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_fund_daily_datas(self, argsDict):
        '''
        场内基金日线行情
        接口：fund_daily
        描述：获取场内基金日线行情，类似股票日行情
        更新：每日收盘后2小时内
        限量：单次最大800行记录，总量不限制
        积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_daily"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.fund_daily(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.fund_daily(trade_date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_tmt_twincome_datas(self, argsDict):
        '''
        台湾电子产业月营收
        接口：tmt_twincome
        描述：获取台湾TMT电子产业领域各类产品月度营收数据。
        '''
        tableName = "tmt_twincome"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "item":
                df = self.pro.tmt_twincome(item='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_tmt_twincomedetail_datas(self, argsDict):
        '''
        台湾电子产业月营收明细
        接口：tmt_twincomedetail
        描述：获取台湾TMT行业上市公司各类产品月度营收情况。
        '''
        tableName = "tmt_twincomedetail"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "date":
                df = self.pro.tmt_twincomedetail(date='{0}'.format(inputCode))
            elif codeType == "item":
                df = self.pro.tmt_twincomedetail(item='{0}'.format(inputCode))
            elif codeType == "symbol":
                df = self.pro.tmt_twincomedetail(symbol='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_bo_monthly_datas(self, argsDict):
        '''
        电影月度票房
        接口：bo_monthly
        描述：获取电影月度票房数据
        数据更新：本月更新上一月数据
        数据历史： 数据从2008年1月1日开始，超过10年历史数据。
        数据权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "bo_monthly"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "date":
                df = self.pro.bo_monthly(date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_bo_weekly_datas(self, argsDict):
        '''
        电影周度票房
        接口：bo_weekly
        描述：获取周度票房数据
        数据更新：本周更新上一周数据
        数据历史： 数据从2008年第一周开始，超过10年历史数据。
        数据权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "bo_weekly"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "date":
                df = self.pro.bo_weekly(date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_bo_daily_datas(self, argsDict):
        '''
        电影日度票房
        接口：bo_daily
        描述：获取电影日度票房
        数据更新：当日更新上一日数据
        数据历史： 数据从2018年9月开始，更多历史数据正在补充
        数据权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "bo_daily"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "date":
                df = self.pro.bo_daily(date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_bo_cinema_datas(self, argsDict):
        '''
        影院每日票房
        接口：bo_cinema
        描述：获取每日各影院的票房数据
        数据历史： 数据从2018年9月开始，更多历史数据正在补充
        数据权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "bo_cinema"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "date":
                df = self.pro.bo_cinema(date='{0}'.format(inputCode))
            else:
                return False
            time.sleep(2.5)

            df = df.fillna(value=0)
            datas = df.to_dict("records")

            if len(datas) != 0:

                dataType = self.get_table_column_data_type(tableName)
                try:
                    strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
                    self.mysqlExe.execute(strSql)
                    self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                    self.insert_collect_flag(tableName, codeType, inputCode)
                except Exception as e:
                    print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_weekly_datas(self, argsDict):
        '''
        接口：weekly
        描述：获取A股周线行情
        限量：单次最大3700，总量不限制
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "weekly"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.weekly(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.weekly(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_monthly_datas(self, argsDict):
        '''
        接口：monthly
        描述：获取A股月线数据
        限量：单次最大3700，总量不限制
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "monthly"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "ts_code":
            df = self.pro.monthly(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.monthly(ts_code='', trade_date='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

    def proc_main_opt_basic_datas(self, argsDict):
        '''
        接口：opt_basic
        描述：获取期权合约信息
        积分：用户需要至少200积分才可以调取，但有流量控制，请自行提高积分，积分越多权限越大，具体请参阅积分获取办法
        '''
        tableName = "opt_basic"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return

        if codeType == "exchange":
            df = self.pro.opt_basic(exchange='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSql = "delete from {0} where {1} = '{2}';".format(tableName, codeType, inputCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)


    # -- 直接通过接口返回数据 --

    def proc_rtn_datas_stock_code(self):
        '''
        股票列表
        接口：stock_basic
        描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        '''
        # print(tp.get_table_field_list("stock_basic"))
        try:
            stockInfoList = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,'
                                                                                   'is_hs')
            df = stockInfoList.fillna(value=0)

            for indexs in df.index:
                # print(indexs)
                # xdata = df.loc[indexs].values[0:-1]
                xdata = df.loc[indexs].values[0:-1]
                xdata1 = df.columns
                for x in xdata1:
                    print(x)
                # print(xdata)
                # dd = xdata[2]
                # print(dd)

            # stime = df.timeToMarket

            # print(df["ts_code"][0])

            # print(str(df["ts_code"]))

            # code = df["ts_code"].isin(["000005.SZ"])
            # print(type(code))

            # if "000005.SZ" == str(df.ts_code):
            #     print(df.name)


        except Exception as e:
            print(e)
            return False

