# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2019/1/7 13:37'

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

    #  数据库操作函数
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

    def delete_table_datas(self, tableName, key_word, key_detail):
        try:
            strSql = "delete from {0} where {1} = '{2}'".format(tableName, key_word, key_detail)
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

    def get_datas_for_db_trade_cal(self, exchangeCode, begDate, endDate):
        try:
            strSql = "select distinct cal_date from trade_cal a where 0=0 and a.exchange = '{3}' and is_open=1 and cal_date between {0} and {1} order by cal_date".format(begDate, endDate, exchangeCode)
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_concept(self):
        try:
            strSql = "select code from concept a where 0=0 group by code order by code;"
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

    def get_datas_for_db_fund_basic(self):
        try:
            strSql = "select ts_code from fund_basic group by ts_code order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    def get_datas_for_db_fut_basic(self):
        try:
            strSql = "select ts_code from fut_basic group by ts_code order by ts_code"
            rtnDatas = self.mysqlExe.query(strSql)
            if len(rtnDatas) == 0:
                return False
            else:
                return rtnDatas
        except:
            return False

    #################################################################

    # 业务函数

    def get_tpdatas_trade_cal_2_db(self, exchangeCode, startDate, endDate):
        '''
        股票列表
        接口：trade_cal
        描述：获取各大交易所交易日历数据,默认提取的是上交所
        '''
        tableName = "trade_cal"

        df = self.pro.trade_cal(exchange='{0}'.format(exchangeCode), start_date=startDate, end_date=endDate)
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            strSql = "delete from trade_cal where exchange='{2}' and cal_date between {0} and {1}".format(startDate, endDate, exchangeCode)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def get_tpdatas_stock_basic(self, argsDict):
        '''
        股票列表
        接口：stock_basic
        描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        '''
        # print(tp.get_table_field_list("stock_basic"))
        tableName = "stock_basic"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.stock_basic(exchange='', list_status='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = []
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")

            self.truncate_table(tableName)
            self.insert_collect_flag(tableName, codeType, inputCode)

        except Exception as e:
            print(e)

        return strSqlList

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

    def get_tpdatas_stock_company(self, argsDict):
        '''
        上市公司基本信息
        接口：stock_company
        描述：获取上市公司基础信息
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "stock_company"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.stock_company(exchange='{0}'.format(inputCode), fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,'
                                                         'main_business,business_scope')
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)

        try:
            # self.truncate_table(tableName)

            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_namechange(self, argsDict):
        '''
        股票曾用名
        接口：namechange
        描述：历史名称变更记录
        '''
        tableName = "namechange"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.namechange()
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.truncate_table(tableName)
            # self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_hs_const(self, argsDict):
        '''
        沪深股通成份股
        接口：hs_const
        描述：获取沪股通、深股通成分数据
        '''
        tableName = "hs_const"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.hs_const(hs_type=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = []
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_new_share(self, argsDict):
        '''
        接口：new_share
        描述：获取新股上市列表数据
        限量：单次最大2000条，总量不限制
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''

        tableName = "new_share"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if inputCode != "":
            df = self.pro.new_share(start_date="{0}".format(inputCode))
        else:
            df = self.pro.new_share()

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = []

        try:
            # self.truncate_table(tableName)

            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            if inputCode != "":
                self.delete_table_datas(tableName, codeType, inputCode)
            else:
                self.truncate_table(tableName)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_concept(self, argsDict):
        '''
        接口：concept
        描述：获取概念股分类，目前只有ts一个来源，未来将逐步增加来源
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''

        tableName = "concept"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.concept()
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = []
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            if inputCode != "":
                self.delete_table_datas(tableName, codeType, inputCode)
            else:
                self.truncate_table(tableName)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_concept_detail(self, argsDict):
        '''
        接口：concept_detail
        描述：获取概念股分类明细数据
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''

        tableName = "concept_detail"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.concept_detail(id='{0}'.format(inputCode))
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = []
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            if inputCode != "":
                self.delete_table_datas(tableName, codeType, inputCode)
            else:
                self.truncate_table(tableName)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_daily(self, argsDict):
        '''
        日线行情
        接口：daily
        更新时间：交易日每天15点～16点之间
        描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
        '''
        tableName = "daily"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.daily(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.daily(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_weekly(self, argsDict):
        '''
        接口：weekly
        描述：获取A股周线行情
        限量：单次最大3700，总量不限制
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "weekly"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.weekly(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.weekly(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_monthly(self, argsDict):
        '''
        接口：monthly
        描述：获取A股月线数据
        限量：单次最大3700，总量不限制
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "monthly"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.monthly(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.monthly(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_adj_factor(self, argsDict):
        '''
        接口：adj_factor
        更新时间：早上9点30分
        描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
        '''
        tableName = "adj_factor"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.adj_factor(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.adj_factor(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_daily_basic(self, argsDict):
        '''
        接口：daily_basic
        更新时间：交易日每日15点～17点之间
        描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "daily_basic"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.daily_basic(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.daily_basic(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_suspend(self, argsDict):
        '''
        接口：suspend
        更新时间：不定期
        描述：获取股票每日停复牌信息
        '''
        tableName = "suspend"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.suspend(ts_code=inputCode)
        elif codeType == "suspend_date":
            df = self.pro.suspend(suspend_date=inputCode)
        elif codeType == "resume_date":
            df = self.pro.suspend(resume_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_income(self, argsDict):
        '''
        接口：income
        描述：获取上市公司财务利润表数据
        积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "income"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.income(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.income(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_balancesheet(self, argsDict):
        '''
        接口：balancesheet
        描述：获取上市公司资产负债表
        积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "balancesheet"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.balancesheet(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.balancesheet(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_cashflow(self, argsDict):
        '''
        接口：cashflow
        描述：获取上市公司现金流量表
        积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "cashflow"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.cashflow(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.cashflow(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_forecast(self, argsDict):
        '''
        接口：forecast
        描述：获取业绩预告数据
        权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "forecast"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.forecast(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.forecast(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_express(self, argsDict):
        '''
        接口：express
        描述：获取上市公司业绩快报
        权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "express"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.express(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.express(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_dividend(self, argsDict):
        '''
        接口：dividend
        描述：分红送股数据
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "dividend"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.dividend(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.dividend(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fina_indicator(self, argsDict):
        '''
        接口：fina_indicator
        描述：获取上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回60条记录，可通过设置日期多次请求获取更多数据。
        权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_indicator"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fina_indicator(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.fina_indicator(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fina_audit(self, argsDict):
        '''
        接口：fina_audit
        描述：获取上市公司定期财务审计意见数据
        权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_audit"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fina_audit(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.fina_audit(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fina_mainbz(self, argsDict):
        '''
        接口：fina_mainbz
        描述：获得上市公司主营业务构成，分地区和产品两种方式
        权限：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_mainbz"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fina_mainbz(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.fina_mainbz(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_top10_holders(self, argsDict):
        '''
        接口：top10_holders
        描述：获取上市公司前十大股东数据，包括持有数量和比例等信息。
        '''
        tableName = "top10_holders"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.top10_holders(ts_code=inputCode)
        elif codeType == "period":
            df = self.pro.top10_holders(period=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_top10_floatholders(self, argsDict):
        '''
        接口：top10_floatholders
        描述：获取上市公司前十大流通股东数据。
        '''
        tableName = "top10_floatholders"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.top10_floatholders(ts_code=inputCode)
        elif codeType == "period":
            df = self.pro.top10_floatholders(period=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_pledge_stat(self, argsDict):
        '''
        接口：pledge_stat
        描述：获取股权质押统计数据
        限量：单次最大1000
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "pledge_stat"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.pledge_stat(ts_code=inputCode)
        elif codeType == "ts_code":
            df = self.pro.pledge_stat(ts_code=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_pledge_detail(self, argsDict):
        '''
        接口：pledge_detail
        描述：获取股权质押明细数据
        限量：单次最大1000
        积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "pledge_detail"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.pledge_detail(ts_code=inputCode)
        elif codeType == "ts_code":
            df = self.pro.pledge_detail(ts_code=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_share_float(self, argsDict):
        '''
        接口：share_float
        描述：获取限售股解禁
        限量：单次最大5000条，总量不限制
        积分：120分可调取，每分钟内限制次数，超过5000积分无限制，具体请参阅积分获取办法
        '''
        tableName = "share_float"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.share_float(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.share_float(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_block_trade(self, argsDict):
        '''
        接口：block_trade
        描述：大宗交易
        限量：单次最大1000条，总量不限制
        积分：300积分可调取，每分钟内限制次数，超过5000积分无限制，具体请参阅积分获取办法
        '''
        tableName = "block_trade"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.block_trade(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.block_trade(ann_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_index_basic(self, argsDict):
        '''
        接口：index_basic
        描述：获取指数基础信息。
        '''
        tableName = "index_basic"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "market":
            df = self.pro.index_basic(market=inputCode)
        elif codeType == "market":
            df = self.pro.index_basic(market=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_basic(self, argsDict):
        '''
        接口：fund_basic
        描述：获取公募基金数据列表，包括场内和场外基金
        积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_basic"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "market":
            df = self.pro.fund_basic(market=inputCode)
        elif codeType == "market":
            df = self.pro.fund_basic(market=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_company(self, argsDict):
        '''
        接口：fund_company
        描述：获取公募基金管理人列表
        积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_company"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        df = self.pro.fund_company()
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            if inputCode != "":
                self.delete_table_datas(tableName, codeType, inputCode)
            else:
                self.truncate_table(tableName)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_index_daily(self, argsDict):
        '''
        接口：index_daily
        描述：获取指数每日行情，还可以通过bar接口获取。由于服务器压力，目前规则是单次调取最多取2800行记录，可以设置start和end日期补全。指数行情也可以通过通用行情接口获取数据．
        权限：用户需要累积200积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "index_daily"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.index_daily(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.index_daily(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_index_weight(self, argsDict):
        '''
        接口：index_weight
        描述：获取各类指数成分和权重，月度数据 ，如需日度指数成分和权重，请联系 waditu@163.com
        来源：指数公司网站公开数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "index_weight"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "index_code":
            df = self.pro.index_weight(index_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.index_weight(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_nav(self, argsDict):
        '''
        接口：fund_nav
        描述：获取公募基金净值数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_nav"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fund_nav(ts_code=inputCode)
        elif codeType == "end_date":
            df = self.pro.fund_nav(end_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_div(self, argsDict):
        '''
        接口：fund_div
        描述：获取公募基金分红数据
        积分：用户需要至少400积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_div"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fund_div(ts_code=inputCode)
        elif codeType == "ann_date":
            df = self.pro.fund_div(ann_date=inputCode)
        elif codeType == "ex_date":
            df = self.pro.fund_div(ex_date=inputCode)
        elif codeType == "pay_date":
            df = self.pro.fund_div(pay_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_portfolio(self, argsDict):
        '''
        接口：fund_portfolio
        描述：获取公募基金持仓数据，季度更新
        积分：用户需要至少1000积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_portfolio"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fund_portfolio(ts_code=inputCode)
        elif codeType == "ts_code":
            df = self.pro.fund_portfolio(ts_code=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fut_basic(self, argsDict):
        '''
        接口：fut_basic
        描述：获取期货合约列表数据
        限量：单次最大10000
        积分：用户需要至少200积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fut_basic"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "exchange":
            df = self.pro.fut_basic(exchange=inputCode)
        elif codeType == "fut_type":
            df = self.pro.fut_basic(fut_type=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fund_daily(self, argsDict):
        '''
        接口：fund_daily
        描述：获取场内基金日线行情，类似股票日行情
        更新：每日收盘后2小时内
        限量：单次最大800行记录，总量不限制
        积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fund_daily"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fund_daily(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.fund_daily(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList

    def get_tpdatas_fut_daily(self, argsDict):
        '''
        接口：fut_daily
        描述：期货日线行情数据
        限量：单次最大2000条，总量不限制
        积分：用户需要至少200积分才可以调取，未来可能调整积分，请尽量多的积累积分。具体请参阅积分获取办法
        '''
        tableName = "fut_daily"
        strSqlList = []

        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        if argsDict["recollect"] == "1":
            self.delete_collect_flag(tableName, codeType, inputCode)

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is False:
            print("接口名称：{0} {1}，已在今天采集，无需再次采集！".format(tableName, inputCode))
            return strSqlList

        if codeType == "ts_code":
            df = self.pro.fut_daily(ts_code=inputCode)
        elif codeType == "trade_date":
            df = self.pro.fut_daily(trade_date=inputCode)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        if len(datas) == 0:
            print("接口名称：{0} {1}，无任何返回记录！".format(tableName, inputCode))
            return strSqlList

        dataType = self.get_table_column_data_type(tableName)
        try:
            strSqlList = self.get_insert_sql(tableName, datas, dataType, "N")
            self.delete_table_datas(tableName, codeType, inputCode)
            self.insert_collect_flag(tableName, codeType, inputCode)
        except Exception as e:
            print(e)

        return strSqlList


