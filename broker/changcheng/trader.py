# coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import re
import json
import time
import random
import string
import const as ct
from log import getLogger
from common import get_market
from base.net.client import Client
from base.net.session import SessionClient
from broker.changcheng.crack_bmp import CrackBmp
from broker.changcheng.html_parser import HtmlParser
LOGIN_URL = "https://trade.cgws.com/cgi-bin/user/Login"
STOCK_MONUT = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery"
VALIDATE_IMG_URL = "https://trade.cgws.com/cgi-bin/img/validateimg"
LOGOUT_URL = "https://trade.cgws.com/cgi-bin/user/Login?function=CloseSession"
ACCOUNT_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyAccount"
DEAL_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockBusiness"
CANCEL_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/StockEntrust?function=StockCancel"
STOCK_INFO_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=QueryStockInfo"
HOLDING_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyStock&stktype=0"
SUBMITTED_ORDER_URL = "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=MyStock&stktype=0"
class Trader:
    def __init__(self, account, passwd, sh_id, sz_id):
        self.secuids = {
            ct.MARKET_SH: sh_id,
            ct.MARKET_SZ: sz_id
        }
        self.log = getLogger(__name__)
        self.passwd = passwd
        self.account = account
        self.cookie = None
        self.headers = {
            'Cache-Control': r'max-age=0',
            'Upgrade-Insecure-Requests': r'1',
            'Origin': r'https://trade.cgws.com',
            'Cookie': r'_trs_uv=jsvhvv2t_788_fpgf;',
            'Accept-Encoding': r'gzip, deflate, br',
            'Content-Type': r'application/x-www-form-urlencoded',
            'Referer': r'https://trade.cgws.com/cgi-bin/user/Login',
            'Accept-Language': r'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept':r'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent':r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
        }
        self.client = Client() 
        self.session_client = SessionClient(self.headers)

    def close(self):
        extra_value = "fundAccount=%s; loginType=%s;" % (self.account, 'Z')
        ret, result = self.session_client.logout(LOGOUT_URL, extra_value)
        if ret != 0:
            self.log.error('close session failed, account:%s, reason:%s' % (self.account, result))
        return ret

    def prepare(self):
        #prepare for login
        ret = self.client.prepare(LOGIN_URL)
        if ret != 0:
            self.log.error("prepare data failed: ret=%d" % ret)
            return -10

        #get verify img
        text_body = {"rand": random.random()}
        (ret, tmp_buff) = self.client.get(VALIDATE_IMG_URL, text_body)
        if ret != 0:
            self.log.error("get verified code fail: ret=%d" % ret)
            return -10

        verify_code = CrackBmp().get_verified_code(tmp_buff)
        post_data = {
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
        self.session_client.cookies = self.client.cookie
        extra_value = "fundAccount=%s; loginType=%s;" % (post_data['fundAccount'], post_data['type'])
        (ret, reason) = self.session_client.login(LOGIN_URL, post_data, extra_value)
        if ret != 0: self.log.error('login failed, reason:%s' % reason)
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
        (ret, response) = self.session_client.post(DEAL_URL, post_data)
        self.log.debug("%s action:%s, current price:%s, amount:%s" % (code, action, price, amount))
        if ret != 0:
            self.log.warn("post to url fail: ret=%d" % ret)
            return -10
        #check if has error
        result = response.text
        reg = re.compile(r'.*alert\(\"-(\d{9}).*\)')
        match = reg.search(result)
        if match:
            if match.group(1) == "150906130":
                return ct.NOT_ENOUGH_MONEY, "no enough money"
            elif match.group(1) == "160002006":
                return ct.NOT_DEAL_TIME, "[配售申购]业务，当前时间不允许委托，可委托的时间段为：091500-150000!" 
            elif match.group(1) == "150906090":
                return ct.ALREADY_BUY, "already buy new stock for:%s" % code
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
            match = reg.search(result)
            if match: return ct.SHENGOU_LIMIT, match.group(1)
        #parse the deal id. if not exist return ""
        reg = re.compile(r'alert.*(\d{4})')
        match = reg.search(result)
        if match:
            return 0, match.group(1)
        else:
            return -5, "err happened need check."

    def cancel(self, order_id):
        post_data = {"id": order_id}
        (ret, response) = self.session_client.post(CANCEL_ORDER_URL, post_data)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, None
        # check if has error
        reg = re.compile(r'.*alert.*\[-(\d{6,})\]')
        result = response.text
        match = reg.search(result)
        if match:
            if match.group(1) == "990268040":
                return ct.NOT_RIGHT_ORDER_ID, "order id:%s is not right" % order_id 
            else:
                return ct.OTHER_ERROR, "other error"
        return 0, None

    #query account info
    def accounts(self):
        (ret, response) = self.session_client.get(ACCOUNT_URL)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get accounts url failed"
        return 0, HtmlParser(response).get_account()

    def holdings(self):
        (ret, result) = self.session_client.get(HOLDING_URL)
        if ret != 0:
            logging.warn("get to url fail: ret=%d" % ret)
            return -5, None
        return 0, HtmlParser(result).get_holdings()

    #query orders if order_type = SUBMITTED get the submitted order, 
    #ONGOING get the onging order 
    def orders(self, order_type):
        if order_type == ct.ONGOING:
            query_url = CANCEL_ORDER_URL
        else:
            query_url = SUBMITTED_ORDER_URL
        (ret, result) = self.session_client.get(query_url)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        return 0, HtmlParser(result).get_orders()

    def max_amounts(self, code, price, max_qty):
        "https://trade.cgws.com/cgi-bin/stock/EntrustQuery?function=ajaxMaxAmount&market=0&secuid=0121056913&stkcode=300762&bsflag=B&price=16.280&rand=1551774639858"
        randNum = str(int(time.time())) + "".join(map(lambda x:random.choice(string.digits), range(3)))
        market_id = get_market(code)
        post_data = {
            "function": "ajaxMaxAmount",
            "market": market_id,
            "secuid": self.secuids[market_id],
            "stkcode": code,
            "bsflag": "B",
            "price": price,
            "rand": randNum
        } 
        (ret, response) = self.session_client.post(STOCK_MONUT, post_data)
        if ret != 0:
            self.log.warn("get to url fail: ret=%d" % ret)
            return -5, "get order url failed"
        try:
            stock_info = response.json()
            return int(stock_info[0]['errorCode']), int(stock_info[0]['maxstkqty'])
        except:
            return -6, "stock info can not be json."

if '__main__' == __name__:
    #with open(ct.USER_FILE) as f:
    with open("/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/user.json") as f:
        infos = json.load(f)
    trader = Trader(infos[0]["account"], infos[0]["passwd_encrypted"], infos[0]["secuids_sh"], infos[0]["secuids_sz"])
    if 0 == trader.prepare():
        print(trader.accounts())
        print(trader.holdings())
        print(trader.orders(ct.ONGOING))
        print(trader.orders(ct.SUBMITTED))
        print(trader.orders(ct.SUBMITTED))
        print(trader.deal('002935', 19.38, 8000, 'B'))
        print(trader.max_amounts('002935', 19.38, 0))
        print(trader.close())
