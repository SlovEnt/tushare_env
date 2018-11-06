# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/5 21:42'

import time
import os

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













