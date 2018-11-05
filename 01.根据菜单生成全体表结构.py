# -*- coding: utf-8 -*-
__author__ = 'SlovEnt'
__date__ = '2018/11/4 22:38'

from collections import OrderedDict
from bs4 import BeautifulSoup
import requests
import os
import re

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


# 获取网页信息
def get_html_text(url):
    try:
        r =requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        # print('success')
        return r.text
    except:
        # print('false')
        return False

# 解析网页数据， 获取子项页面链接
def parse_html_sub_page_url(html):

    try :
        soup = BeautifulSoup(html, 'lxml')
        soup = soup.find('nav', class_="sidebar")
        subPageUrlList = []
        for x in soup.find_all('a'):

            if "沪深股票" in str(x):
                continue

            if "doc_id" not in str(x):
                continue

            if "false" in str(x):
                continue

            if "通用行情接口" in str(x):
                continue

            subPageInfo = OrderedDict()
            subPageInfo["url"] = x.get("href")
            subPageInfo["title"] = x.text

            subPageUrlList.append(subPageInfo)

        return subPageUrlList

    except Exception as e :
        return False

def generate_sub_table_sql(subPageUrl, subTitle):
    html = get_html_text(subPageUrl)
    soup = BeautifulSoup(html, 'lxml')
    tableNameRe = "<p>(接口|接口名称)：(.+?)<br/>"
    tableName = re.findall(tableNameRe, str(soup))[0][1]

    strSql = "show tables like '%s'; " % tableName

    rtnDatas = mysqlExe.query(strSql)

    strSql = """CREATE TABLE {0} (        
`busi_date` varchar(64) COLLATE utf8_bin NOT NULL DEFAULT '' COMMENT '采集日期',  %s
PRIMARY KEY (busi_date)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='{1}';
""".format(tableName, subTitle)

    if "公募基金公司" in html :
        tableList = soup.find_all('table')[0].tbody
    else :
        tableList = soup.find_all('table')[1].tbody

    rowList = tableList.find_all("tr") # 定位所有的tr

    fieldStrSql = ""

    for row in rowList:
        # print(str(row))
        fieldInfo = row.find_all("td")
        # print(fieldInfo)

        fieldName = fieldInfo[0].contents[0]
        fieldType = fieldInfo[1].contents[0]

        if len(fieldInfo) == 4:
            fieldDesc = fieldInfo[3].contents[0]
        else:
            fieldDesc = fieldInfo[2].contents[0]

        if fieldType == "str":
            fieldType = "varchar(256)"
            singFieldStrSql = "`{0}` {1} COLLATE utf8_bin NOT NULL DEFAULT '' COMMENT '{2}',".format(
                fieldName,
                fieldType,
                fieldDesc)

        elif fieldType == "int":
            fieldType = "int(128)"
            singFieldStrSql = "`{0}` {1} COLLATE utf8_bin COMMENT '{2}',".format(
                fieldName,
                fieldType,
                fieldDesc)

        elif fieldType == "float":
            fieldType = "float(32,4)"
            singFieldStrSql = "`{0}` {1} COLLATE utf8_bin COMMENT '{2}',".format(
                fieldName,
                fieldType,
                fieldDesc)


        fieldStrSql = "%s\n%s" % (fieldStrSql, singFieldStrSql)

    strSql = strSql % fieldStrSql

    return strSql,len(rtnDatas)

def main():

    url_basic = 'https://tushare.pro'
    url_main = 'https://tushare.pro/document/2'
    html = get_html_text(url_main)
    if html is not False:
        subPageUrlList = parse_html_sub_page_url(html)

    for subItem in subPageUrlList:
        subPageUrl = "%s%s" % (url_basic, subItem["url"])
        subTitle = subItem["title"]
        print(subTitle, subPageUrl)

        strSql,cnt = generate_sub_table_sql(subPageUrl, subTitle)

        if cnt == 0:
            mysqlExe.execute(strSql.replace("%","%%"))




if __name__ == '__main__':
    main()
