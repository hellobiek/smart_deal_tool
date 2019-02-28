# coding=utf-8
import json
import gevent
import datetime
from datetime import datetime
import ccalendar
import const as ct
import pandas as pd
import tushare as ts
from cmysql import CMySQL
from trader import Trader
from log import getLogger
from common import is_trading_time
STOCK_NUM = 30
IPO_DATE_HEAD = 'ipo_date'
IPO_CODE_HEAD = 'xcode'
IPO_PRICE_HEAD = 'price'
logger = getLogger(__name__)
class CTrader:
    def __init__(self, dbinfo, fpath = ct.USER_FILE):
        self.mysql_client = CMySQL(dbinfo)
        self.cal_client = ccalendar.CCalendar(without_init = True)
        with open(fpath) as f: infos = json.load(f)
        self.traders = list()
        for info in infos:
            self.traders.append(Trader(info["account"], info["passwd_encrypted"], info["secuids_sh"], info["secuids_sz"]))
        self.buy_succeed_date = ""

    def init(self):
        for i in range(len(self.traders)):
            if 0 != self.traders[i].prepare(): return False
        return True

    def buy_new_stock(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day():
                    if is_trading_time():
                        _today = datetime.now().strftime('%Y-%m-%d')
                        if self.buy_succeed_date != _today:
                            if not self.init(): raise Exception("trader login failed")
                            n_list = self.get_new_stock_list()
                            if len(n_list) == 0:
                                logger.info("no new stock for %s." % _today)
                                self.buy_succeed_date = _today
                            succeed = True
                            for stock in n_list:
                                for i in range(len(self.traders)):
                                    ret, amount = self.traders[i].max_amounts(stock[0], stock[1])
                                    if 0 == ret:
                                        ret, msg = self.traders[i].deal(stock[0], stock[1], amount, "B")
                                        if ret != 0 and ret != ct.ALREADY_BUY:
                                            logger.error("buy new stock:%s amount:%s for %s error, msg:%s, ret:%s" % (stock, amount, _today, msg, ret))
                                            succeed = False
                                        elif ret == 0:
                                            logger.info("buy new stock:%s amount:%s for %s succeed." % (stock, amount, _today))
                                        elif ret == ct.ALREADY_BUY:
                                            logger.info("already buy new stock:%s amount:%s for %s, no use to buy more." % (stock, amount, _today))
                            if True == succeed: 
                                self.buy_succeed_date = _today
            except Exception as e:
                logger.error(e)
            gevent.sleep(sleep_time)
    
    def get_new_stock_list(self):
        stock_list = []
        top_stocks_info = ts.new_stocks().head(STOCK_NUM)
        stocks_info = top_stocks_info[[IPO_CODE_HEAD, IPO_DATE_HEAD, IPO_PRICE_HEAD]]
        for i in range(STOCK_NUM):
            stock_date = stocks_info.at[i, IPO_DATE_HEAD]
            if pd.to_datetime(stock_date).strftime('%Y-%m-%d') == datetime.now().strftime('%Y-%m-%d'):
                code = stocks_info.at[i, IPO_CODE_HEAD]
                price = stocks_info.at[i, IPO_PRICE_HEAD]
                stock_list.append((code, price))
        return stock_list
