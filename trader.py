#!/usr/local/bin/python3
# coding=utf-8
import json
import re
import time
import random
import string
import const as ct
from lxml import html
from html_parser import *
from client import Client
from log import getLogger
from common import get_verified_code

VALIDATE_IMG_URL = "https://trade.cgws.com/cgi-bin/img/validateimg"
LOGIN_URL = "https://trade.cgws.com/cgi-bin/user/Login"
DEAL_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockBusiness"
CANCEL_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockCancel"
SUBMITTED_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyStock&stktype=0"
ACCOUNT_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyAccount"
HOLDING_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyStock&stktype=0"
STOCK_INFO_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=QueryStockInfo"
STOCK_MONUT = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery"

class Trader:
    def __init__(self, account, passwd, sh_id, sz_id):
        self.secuids = {
            ct.MARKET_SH: sh_id,
            ct.MARKET_SZ: sz_id
        }
        self.log = getLogger(__name__)
        self.passwd = passwd
        self.account = account
        self.client = Client()
        ret = self.prepare()
        if ret != 0: raise Exception("login failed, return value:%s" % ret)

    def prepare(self):
        #preprea for login
        ret= self.client.prepare()
        if ret != 0:
            self.log.warn("prepare fail: ret=%d" % ret)
            return -5
        #get verify img
        text_body = {"rand": random.random()}
        (ret, tmp_buff) = self.client.get(VALIDATE_IMG_URL, text_body)
        if ret != 0:
            self.log.warn("get verified code fail: ret=%d" % ret)
            return -10
        verify_code = get_verified_code(tmp_buff)
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
        (ret, _) = self.client.post(LOGIN_URL, post_data)
        return ret

    #action: "B" for buy, "S" for sell
    def deal(self, code, price, amount, action):
        market_id = get_market(code) 
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
            "stkcode": code,
            "stockName":"",
            "price": price,
            "fundavl": "1.00",
            "maxBuy": maxBuy,
            "amount": amount
        }
        (ret, result) = self.client.post(DEAL_URL, post_data)
        self.log.debug("%s action: %s, current price:%s, amount:%s" % (code, action, price, amount))
        if ret != 0:
            self.log.warn("post to url fail: ret=%d" % ret)
            return -10
        #check if has error
        reg = re.compile(r'.*alert.*\[-(\d{6,})\]')
        match = reg.search(result.decode('gbk', "ignore"))
        if match:
            if match.group(1) == "150906130":
                return ct.NOT_ENOUGH_MONEY, "no enough money"
            elif match.group(1) == "150906135":
                return ct.NOT_ENOUGH_STOCK, "no enough stocks for sell"
            elif match.group(1) == "999003088":
                return ct.SETTLEMENT_TIME,  "deal forbidden"
            elif match.group(1) == "990297020":
                return ct.NOT_DEAL_TIME, "not in deal time"
            elif match.group(1) == "990265060":
                return ct.NOT_CORRECT_PRICE, "price is not right"
            elif match.group(1) == "150906090":
                return ct.REPEATED_SHENGOU, "new stock can not be duplicated delegation."
            elif match.group(1) == "990221020":
                return ct.NO_SUCH_CODE, "not such code:%s." % code
            else:
                return ct.OTHER_ERROR, "other err"
        else:
            reg = re.compile('.*alert.*新股申购数量超出.*\[(\d{3,})\]')
            match = reg.search(result.decode('gbk', "ignore"))
            if match:
                return ct.SHENGOU_LIMIT, match.group(1)
            else:
                return ct.OTHER_ERROR, "other error"
        #parse the deal id. if not exist return ""
        reg = re.compile(r'alert.*(\d{4})')
        match = reg.search(result.decode("gbk", "ignore"))
        if match:
            return 0, match.group(1)
        else:
            return -5, "err happened need check."

    def cancel(self, order_id):
        post_data = {"id": order_id}
        (ret, result) = self.client.post(CANCEL_ORDER_URL, post_data)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, None
        # check if has error
        reg = re.compile(r'.*alert.*\[-(\d{6,})\]')
        match = reg.search(result.decode("gbk", "ignore"))
        if match:
            if match.group(1) == "990268040":
                return ct.NOT_RIGHT_ORDER_ID, "order id:%s is not right" % order_id 
            else:
                return ct.OTHER_ERROR, "other error"
        return 0, None

    #query account info
    def accounts(self):
        (ret, result) = self.client.get(ACCOUNT_URL)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get accounts url failed"
        return 0, html_parser(result).get_account()

    def holdings(self):
        (ret, result) = self.client.get(HOLDING_URL)
        if ret != 0:
            logging.warn("get to url fail: ret=%d" % ret)
            return -5, None
        return 0, html_parser(result).get_holdings()

    #query orders if order_type = SUBMITTED get the submitted order, 
    #ONGOING get the onging order 
    def orders(self, order_type):
        if order_type == ct.ONGOING:
            query_url = CANCEL_ORDER_URL
        else:
            query_url = SUBMITTED_ORDER_URL
        (ret, result) = self.client.get(query_url, "")
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        return 0, html_parser(result).get_onging_orders()

    def max_amounts(self, code, price):
        randNum = str(int(time.time())) + "".join(map(lambda x:random.choice(string.digits), range(3)))
        market_id = get_market(code)
        text_body = {
            "function": "ajaxMaxAmount",
            "market": market_id,
            "secuid": self.secuids[market_id],
            "stkcode": code,
            "bsflag": "B",
            "price": price,
            "rand": randNum
        } 
        (ret, result) = self.client.get(STOCK_MONUT, text_body)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        stock_info = json.loads(result)
        return int(stock_info[0]['errorCode']), stock_info[0]['maxstkqty']

if '__main__' == __name__:
    with open(ct.USER_FILE) as f:
        infos = json.load(f)
    trader = Trader(infos[0]["account"], infos[0]["passwd_encrypted"], infos[0]["secuids_sh"], infos[0]["secuids_sz"])
    print(trader.deal('002321', 6.27, 100, 'S'))
    print(trader.accounts())
    print(trader.holdings())
    print(trader.orders(ct.ONGOING))
    print(trader.orders(ct.SUBMITTED))
    print(trader.orders(ct.SUBMITTED))
