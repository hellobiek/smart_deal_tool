#!/usr/bin/python
# coding=utf-8
LOGIN_URL = "https://trade.cgws.com/cgi-bin/user/Login?function=tradeLogout"
ENTRUST_QUERY = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyAccount"
NEW_STOCK_URL = "http://newstock.cfi.cn/"
FUTU_HOST = "172.18.3.101"
FUTU_PORT = 11111
USER_FILE = "user.json"
SLEEP_TIME = 3
SHORT_SLEEP_TIME = 1
SLEEP_INTERVAL = 30
##########stock type##########
MARKET_ALL = 0
MARKET_SZ = 1
MARKET_SH = 2
MARKET_CYB = 3
SUB_NEW_STOCK = 4 
SZ50 = 5
HS300 = 6
ZZ500 = 7
MSCI = 8
MARKET_ELSE = 10
##########order type##########
SUBMITTED = 0
ONGOING = 1
##############################
DB_INFO = {'user':'root',  
           'password':'123456',  
           'host':'localhost',  
           'database':'stock'}
DB_NAME = 'stock'
DB_USER = 'root'
DB_PASSWD = '123456'
DB_HOSTNAME = 'localhost'
UTF8 = "utf8"
SQL = "select * from %s"
RETRY_TIMES = 3
START_DATE = '2017-01-01'
STOCK_LIST = ["SZ50","ZZ500","HS300","MSCI"]
INDEX_LIST = ["000016_SH","000905_SH","000300_SH","000001_SH","399001_SZ","399006_CY"]
AVERAGE_INDEX_LIST = ["700001_ALL", "700002_SH", "700003_SZ", "700004_CYB", "700005_SZ50", "700006_ZZ500", "700007_HS300", "700008_MSCI"]
REAL_INDEX_LIST = ['sh', 'sz', 'cyb', 'sz50', 'hs300', 'zxb', 'zh500']
##############################
class gw_ret_code:
    # 150906130资金不足
    # 150906135股数不够
    # 长城的出错都是这个鸟样 alert("-990297020[-990297020]，出错了就反馈空的订单号，看看是不是自己定义一些exception来搞
    NOT_ENOUGH_MONEY = 1
    NOT_ENOUGH_STOCK = 2
    SETTLEMENT_TIME = 3 #999003088
    NOT_DEAL_TIME =4 #990297020
    NOT_RIGHT_ORDER_ID = 5 #990268040订单号不对
    LOGIN_FAIL = 100 #login fail
    OTHER_ERROR = 999
