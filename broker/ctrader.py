# coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import json
import gevent
import datetime
from datetime import datetime
import lxml.html
import ccalendar
import const as ct
import pandas as pd
from lxml import etree
from cmysql import CMySQL
from common import is_trading_time
from base.clog import getLogger
from broker.changcheng.trader import Trader
from pandas.compat import StringIO
class CTrader:
    def __init__(self, dbinfo, fpath = ct.USER_FILE):
        self.logger = getLogger(__name__)
        self.mysql_client = CMySQL(dbinfo)
        self.cal_client = ccalendar.CCalendar(without_init = True)
        self.traders = list()
        with open(fpath) as f: infos = json.load(f)
        for info in infos:
            self.traders.append(Trader(info["account"], info["passwd_encrypted"], info["secuids_sh"], info["secuids_sz"]))
        self.buy_succeed_date = ""

    def init(self):
        for i in range(len(self.traders)):
            if 0 != self.traders[i].prepare(): return False
        return True

    def close(self):
        for i in range(len(self.traders)):
            if 0 != self.traders[i].close(): return False
        return True

    def buy_new_stock(self, sleep_time):
        while True:
            try:
                self.logger.debug("enter buy_new_stock")
                if self.cal_client.is_trading_day():
                    if is_trading_time():
                        _today = datetime.now().strftime('%Y-%m-%d')
                        if self.buy_succeed_date != _today:
                            n_list = self.get_new_stock_list()
                            if len(n_list) == 0:
                                self.logger.info("no new stock for %s." % _today)
                                self.buy_succeed_date = _today
                            else:
                                if not self.init(): raise Exception("trader login failed")
                                succeed = True
                                for stock in n_list:
                                    for i in range(len(self.traders)):
                                        ret, amount = self.traders[i].max_amounts(stock[0], stock[1], stock[2])
                                        if 0 != ret: 
                                            succeed = False
                                        else:
                                            if amount == 0:
                                                self.logger.info("new stock:%s max amount is:%s for %s succeed." % (stock, amount, _today))
                                            else:
                                                ret, msg = self.traders[i].deal(stock[0], stock[1], amount, "B")
                                                if ret != 0 and ret != ct.ALREADY_BUY:
                                                    self.logger.error("buy new stock:%s amount:%s for %s error, msg:%s, ret:%s" % (stock, amount, _today, msg, ret))
                                                    succeed = False
                                                elif ret == 0:
                                                    self.logger.info("buy new stock:%s amount:%s for %s succeed." % (stock, amount, _today))
                                                elif ret == ct.ALREADY_BUY:
                                                    self.logger.info("already buy new stock:%s amount:%s for %s, no use to buy more." % (stock, amount, _today))
                                if succeed:
                                    if self.close(): self.buy_succeed_date = _today
            except Exception as e:
                self.close()
                self.logger.error(e)
            gevent.sleep(sleep_time)

    def get_new_stocks_from_sina(self):
        url = "http://vip.stock.finance.sina.com.cn/corp/view/vRPD_NewStockIssue.php?page=1&cngem=0&orderBy=NetDate&orderType=desc"
        cols = ['code', 'xcode', 'name', 'ipo_date', 'issue_date', 'amount', 'markets', 'price', 'pe', 'limit', 'funds', 'ballot']
        try:
            html = lxml.html.parse(url)
            res = html.xpath('//table[@id=\'NewStockTable\']')
            if len(res) == 0: return pd.DataFrame()
            sarr = [etree.tostring(node).decode('utf-8') for node in res]
            sarr = ''.join(sarr)
            sarr = sarr.replace('<font color="red">*</font>', '')
            sarr = '<table>%s</table>'%sarr
            df = pd.read_html(StringIO(sarr), skiprows=[0, 1, 2])[0]
            df = df.drop([df.columns[idx] for idx in [12, 13, 14]], axis=1)
            df.columns = cols
            df['code'] = df['code'].map(lambda x : str(x).zfill(6))
            df['xcode'] = df['xcode'].map(lambda x : str(x).zfill(6))
        except Exception as e:
            time.sleep(1)
            self.logger.error(e)
        else:
            return df
        
    def get_new_stock_list(self):
        stock_list = []
        stocks_info = self.get_new_stocks_from_sina()
        if stocks_info.empty: return stock_list 
        today_ = datetime.now().strftime('%Y-%m-%d')
        stocks_info = stocks_info.loc[stocks_info.ipo_date == today_]
        stocks_info = stocks_info.reset_index(drop = True)
        for index, row in stocks_info.iterrows():
            code = row['xcode']
            price = row['price']
            max_qty = row['limit']
            stock_list.append((code, price, max_qty))
        return stock_list

if __name__ == "__main__":
    ctrader = CTrader(ct.DB_INFO)
    ctrader.buy_new_stock(10)
