# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/5 21:42'

import time
import os
from collections import OrderedDict

class Tushare_Proc(object):

    def __init__(self, pro, mysqlExe):
        self.pro = pro
        self.mysqlExe = mysqlExe
        self.busiDate = time.strftime('%Y%m%d', time.localtime(time.time()))

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

    def get_datas_for_db_trade_cal(self,begDate, endDate):
        try:
            strSql = "select cal_date from trade_cal a where 0=0 and a.exchange in ('SSE','SZSE') and is_open=1 and cal_date between {0} and {1} order by cal_date".format(begDate, endDate)
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


    #################################################################


    def proc_main_stock_basic_datas(self):
        '''
        股票列表
        接口：stock_basic
        描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        '''
        # print(tp.get_table_field_list("stock_basic"))
        tableName = "stock_basic"
        df = self.pro.stock_basic(exchange='', list_status='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            self.truncate_table(tableName)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_stock_company_datas(self):
        '''
        上市公司基本信息
        接口：stock_company
        描述：获取上市公司基础信息
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "stock_company"
        df = self.pro.stock_company(exchange='', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = self.get_table_column_data_type(tableName)
        try:
            self.truncate_table(tableName)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_trade_cal_datas(self, startDate, endDate):
        '''
        股票列表
        接口：trade_cal
        描述：获取各大交易所交易日历数据,默认提取的是上交所
        '''
        tableName = "trade_cal"
        df = self.pro.trade_cal(exchange='', start_date=startDate, end_date=endDate)
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            strSql = "delete from trade_cal where cal_date between {0} and {1}".format(startDate, endDate)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_hs_const_datas(self, hsType):
        '''
        沪深股通成份股
        接口：hs_const
        描述：获取沪股通、深股通成分数据
        '''
        tableName = "hs_const"
        df = self.pro.hs_const(hs_type=hsType)
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            strSql = "delete from hs_const where hs_type = '{0}'".format(hsType)
            self.mysqlExe.execute(strSql)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_new_share_datas(self):
        '''
        接口：new_share
        描述：获取新股上市列表数据
        限量：单次最大2000条，总量不限制
        积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "new_share"
        df = self.pro.new_share()
        df = df.fillna(value=0)
        datas = df.to_dict("records")
        dataType = []
        try:
            self.truncate_table(tableName)
            self.insert_new_datas_2_db(tableName, datas, dataType, "N")

        except Exception as e:
            print(e)

    def proc_main_daily_datas(self, trdDate):
        '''
        日线行情
        接口：daily
        更新时间：交易日每天15点～16点之间
        描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
        '''
        tableName = "daily"
        df = self.pro.daily(trade_date=trdDate)
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, 'trade_date', trdDate)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, 'trade_date', trdDate)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, trdDate))

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

        if codeType == "ts_code":
            df = self.pro.adj_factor(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.adj_factor(ts_code='', trade_date='{0}'.format(inputCode))

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

    def proc_main_suspend_datas(self, tsCode):
        '''
        停复牌信息
        接口：suspend
        更新时间：不定期
        描述：获取股票每日停复牌信息
        '''
        tableName = "suspend"
        df = self.pro.suspend(ts_code='{0}'.format(tsCode), suspend_date='', resume_date='', fiedls='ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type')
        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, 'ts_code', tsCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, 'ts_code', tsCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, tsCode))

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

        if codeType == "ts_code":
            df = self.pro.daily_basic(ts_code='{0}'.format(inputCode), trade_date='')
        elif codeType == "trade_date":
            df = self.pro.daily_basic(ts_code='', trade_date='{0}'.format(inputCode))

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

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

        if codeType == "ts_code":
            df = self.pro.income(ts_code='{0}'.format(inputCode), start_date='', end_date='')
        elif codeType == "trade_date":
            df = self.pro.income(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

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

        if codeType == "ts_code":
            df = self.pro.balancesheet(ts_code='{0}'.format(inputCode), start_date='', end_date='')
        elif codeType == "trade_date":
            df = self.pro.balancesheet(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

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

        if codeType == "ts_code":
            df = self.pro.cashflow(ts_code='{0}'.format(inputCode), start_date='', end_date='')
        elif codeType == "trade_date":
            df = self.pro.cashflow(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

        df = df.fillna(value=0)
        datas = df.to_dict("records")

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if len(datas) != 0 and rtnMsg is True:

            dataType = self.get_table_column_data_type(tableName)
            try:
                self.insert_new_datas_2_db(tableName, datas, dataType, "N")
                self.insert_collect_flag(tableName, codeType, inputCode)
            except Exception as e:
                print(e)
        else:
            print("接口名称：{0} {1}，无任何返回记录，或已在今天采集，无需再次采集！".format(tableName, inputCode))

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

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.forecast(ts_code='{0}'.format(inputCode), start_date='', end_date='')
            elif codeType == "trade_date":
                df = self.pro.forecast(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

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

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.express(ts_code='{0}'.format(inputCode), start_date='', end_date='')
            elif codeType == "trade_date":
                df = self.pro.express(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

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

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.dividend(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.dividend(ts_code='', ann_date='{0}'.format(inputCode))

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

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.fina_indicator(ts_code='{0}'.format(inputCode))
            elif codeType == "trade_date":
                df = self.pro.fina_indicator(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

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

    def proc_main_fina_audit_datas(self, argsDict):
        '''
        财务审计意见
        接口：fina_audit
        描述：获取上市公司定期财务审计意见数据
        权限：用户需要至少300积分才可以调取，具体请参阅积分获取办法
        '''
        tableName = "fina_indicator"
        # print(argsDict)
        codeType = argsDict["codeType"]
        inputCode = argsDict["inputCode"]

        rtnMsg = self.select_collect_flag(tableName, codeType, inputCode)

        if rtnMsg is True:

            if codeType == "ts_code":
                df = self.pro.fina_audit(ts_code='{0}'.format(inputCode), fields="ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign")
            elif codeType == "trade_date":
                df = self.pro.fina_audit(ts_code='', start_date='{0}'.format(inputCode), end_date='{0}'.format(inputCode))

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



