#!/usr/bin/python
# coding=utf-8
import json,random,re,string,time
from lxml import html
from log import getLogger
from client import Client
from stock import Stock
from html_parser import html_parser
from common import get_verified_code
from const import MARKET_SH,MARKET_SZ,gw_ret_code,USER_FILE,SUBMITTED,ONGOING

VALIDATE_IMG_URL = "https://trade.cgws.com/cgi-bin/img/validateimg"
LOGIN_URL = "https://trade.cgws.com/cgi-bin/user/Login"
DEAL_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockBusiness"
CANCEL_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockCancel"
SUBMITTED_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyStock&stktype=0"
ACCOUNT_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyAccount"
STOCK_INFO_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=QueryStockInfo"
STOCK_MONUT = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery"

class StockAI:
    def __init__(self, account, passwd, sh_id, sz_id):
        self.stocks = []
        self.secuids = {
            MARKET_SH: sh_id,
            MARKET_SZ: sz_id
        }
        self.passwd = passwd
        self.account = account
        self.client = Client()
        self.log = getLogger(__name__)

    #get the cooikie from stock
    def prepare(self):
        #preprea for login
        (ret, result) = self.client.prepare()
        if ret != 0:
            self.log.warn("get verified code fail: ret=%d" % ret)
            return -5
        #get verify img
        text_body = {
            "rand": random.random()
        }
        (ret, tmp_buff) = self.client.get(VALIDATE_IMG_URL, text_body)
        if ret != 0:
            self.log.warn("get verified code fail: ret=%d" % ret)
            return -10
        verify_code = get_verified_code(tmp_buff)
        ##################login######################
        post_data={
            'ticket': verify_code,
            'retUrl':'',
            'password': self.passwd,
            'mac': '',
            'password_Controls': 'normal',
            'type':'Z',
            'fundAccount': self.account,
            'isSaveAccount':'1',
            'normalpassword':'',
        }
        (ret, result) = self.client.post(LOGIN_URL, post_data)
        if ret != 0:
            return -15
        return 0

    def deal(self, stock, deal_price, amount, action):
        ret = self.prepare()
        if ret != 0:
            return gw_ret_code.LOGIN_FAIL, "login failed"
        self.log.info("%s action: %s, current price:%s deal price:%s, amount:%s" % (stock.code,action,stock.price,deal_price,amount))
        market_id  = stock.market
        up_limit = 0 
        down_limit = 0
        secuid = self.secuids[market_id]
        maxBuy = 0
        post_data = {
            "type": action,
            "market": market_id,
            "up_limit": up_limit,
            "down_limit": down_limit,
            "stktype": "0",
            "secuid": secuid,
            "stkcode": stock.code,
            "stockName":"",
            "price": deal_price,
            "fundavl": "1.00",
            "maxBuy": maxBuy,
            "amount": amount
        }
        (ret, result) = self.client.post(DEAL_URL, post_data)
        if ret != 0:
            self.log.warn("post to url fail: ret=%d" % ret)
            return -10;
        #check if has error
        reg = re.compile(ur'.*alert.*\[-(\d{6,})\]')
        match = reg.search(result)
        if match:
            if match.group(1) == "150906130":
                return gw_ret_code.NOT_ENOUGH_MONEY, "no enough money"
            elif match.group(1) == "150906135":
                return gw_ret_code.NOT_ENOUGH_STOCK, "no enough stocks for sell"
            elif match.group(1) == "999003088":
                return gw_ret_code.SETTLEMENT_TIME,  "deal forbidden"
            elif match.group(1) == "990297020":
                return gw_ret_code.NOT_DEAL_TIME, "not in deal time"
            else:
                return gw_ret_code.OTHER_ERROR, "other err"
        #parse the deal id. if not exist return ""
        self.log.info(result.decode("gbk"))
        reg = re.compile(ur'alert.*(\d{4})')
        match = reg.search(result)
        if match:
            self.log.info("deal id :%s" % match.group(1))
            return 0, match.group(1)
        else:
            return -5, "err happened need check."

    def cancel(self, order_id):
        ret = self.prepare()
        if ret != 0:
            return  gw_ret_code.LOGIN_FAIL, "login failed"
        ############ post buy order #######################
        post_data={
            "id": order_id
        }
        (ret, result) = self.client.post(CANCEL_ORDER_URL, post_data)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, None
        # check if has error
        reg = re.compile(ur'.*alert.*\[-(\d{6,})\]')
        match = reg.search(result)
        if match:
            if match.group(1) == "990268040":
                return gw_ret_code.NOT_RIGHT_ORDER_ID, "order id:%s is not right" % order_id 
            else:
                return gw_ret_code.OTHER_ERROR, "other error"
        return 0, None

    #query account info
    def accounts(self):
        ret = self.prepare()
        if ret != 0:
            return  gw_ret_code.LOGIN_FAIL, "login failed"
        (ret, result) = self.client.get(ACCOUNT_URL, "")
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get accounts url failed"
        return 0, html_parser(result).get_account()

    #query orders if order_type = SUBMITTED get the submitted order, 
    #ONGOING get the onging order 
    def orders(self, order_type):
        ret = self.prepare()
        if ret != 0:
            return  gw_ret_code.LOGIN_FAIL, "login failed"
        if order_type == ONGOING:
            query_url = CANCEL_ORDER_URL
        else:
            query_url = SUBMITTED_ORDER_URL
        (ret, result) = self.client.get(query_url, "")
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        return 0, html_parser(result).get_onging_orders()

    #query max stock for new stock first appear in market 
    def amounts(self, stock):
        ret = self.prepare()
        if ret != 0:
            return  gw_ret_code.LOGIN_FAIL, "login failed"
        randNum = str(int(time.time())) + "".join(map(lambda x:random.choice(string.digits), range(3)))
        text_body = {
            "function": "ajaxMaxAmount",
            "market": stock.market,
            "secuid": self.secuids[stock.market],
            "stkcode": stock.code,
            "bsflag": "B",
            "price": stock.price,
            "rand": randNum
        } 
        (ret, result) = self.client.get(STOCK_MONUT, text_body)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        stock_info = json.loads(result)
        return int(stock_info[0]['errorCode']), stock_info[0]['maxstkqty'] 
   
if '__main__' == __name__:
    with open(USER_FILE) as f:
        quants = json.load(f)
    stockAI = StockAI(quants[0]["account"], quants[0]["passwd_encrypted"], quants[0]["secuids_sh"], quants[0]["secuids_sz"])
    stock = Stock('300648','星云股份','15.74') 
    ret, num = stockAI.amounts(stock)
    print stockAI.deal(stock, stock.price, num, "B") 
