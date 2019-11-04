#-*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from cmysql import CMySQL
from cindex import CIndex
from datetime import datetime
from rstock import RIndexStock
from ccalendar import CCalendar
from base.clog import getLogger
from common import create_redis_obj
from base.cdate import get_day_nday_ago, get_dates_array
class BullStockRatio(object):
    def __init__(self, index_code, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo = dbinfo
        self.index_code = index_code
        self.cal_client = CCalendar(dbinfo = dbinfo, redis_host = redis_host, without_init = True)
        self.index_obj = CIndex(index_code, dbinfo = self.dbinfo, redis_host = redis_host)
        self.db_name = self.index_obj.get_dbname(index_code)
        self.logger = getLogger(__name__)
        self.ris = RIndexStock(dbinfo, redis_host)
        self.bull_stock_ratio_table = self.get_table_name()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client = CMySQL(self.dbinfo, dbname = self.db_name, iredis = self.redis)
        if not self.create(): raise Exception("create emotion table failed")

    def delete(self):
        self.mysql_client.delete(self.bull_stock_ratio_table)

    def get_table_name(self):
        return "%s_%s" % (self.db_name, ct.BULLSTOCKRATIO_TABLE)

    def create(self):
        if self.bull_stock_ratio_table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, ratio float, PRIMARY KEY (date))' % self.bull_stock_ratio_table
            if not self.mysql_client.create(sql, self.bull_stock_ratio_table): return False
        return True

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def get_components(self, cdate):
        df = self.index_obj.get_components_data(cdate)
        if df is None: return list()
        if df.empty: return list()
        if self.index_code == '000001': df = df[df.code.str.startswith('6')]
        return df.code.tolist()

    def get_data(self, mdate):
        return self.ris.get_data(mdate)

    def update(self, end_date = None, num = 30):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        #start_date = "2005-05-26"
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        succeed = True
        code_list = self.get_components(end_date)
        if 0 == len(code_list):
            self.logger.error("%s code_list for %s is empty" % (end_date, self.index_code))
            return False
        for mdate in get_dates_array(start_date, end_date):
            if self.cal_client.is_trading_day(mdate):
                if not self.set_ratio(code_list, mdate):
                    self.logger.error("set %s score for %s set failed" % (self.index_code, mdate))
                    succeed = False
        return succeed

    def get_profit_stocks(self, df):
        data = df[df.profit >= 0]
        return data.code.tolist()

    def get_ratio(self, mdate):
        sql = "select * from %s where date = \"%s\"" % (self.get_table_name(), mdate)
        return self.mysql_client.get(sql)

    def get_ratio_between(self, start_date, end_date):
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (self.get_table_name(), start_date, end_date)
        return self.mysql_client.get(sql)

    def set_ratio(self, now_code_list, cdate = datetime.now().strftime('%Y-%m-%d')):
        if self.is_date_exists(self.bull_stock_ratio_table, cdate):
            self.logger.debug("existed date:%s, date:%s" % (self.bull_stock_ratio_table, cdate))
            return True
        code_list = self.get_components(cdate)
        if len(code_list) == 0: code_list = now_code_list
        df = self.get_data(cdate)
        if df is None: return False
        if df.empty: return False
        df = df[df.code.isin(code_list)]
        if df.empty: return True
        profit_code_list = self.get_profit_stocks(df)
        bull_stock_num = len(profit_code_list)
        bull_ration = 100 * bull_stock_num / len(df)
        data = {'date':[cdate], 'ratio':[bull_ration]}
        df = pd.DataFrame.from_dict(data)
        if self.mysql_client.set(df, self.bull_stock_ratio_table):
            return self.redis.sadd(self.bull_stock_ratio_table, cdate)
        return False

if __name__ == '__main__':
    cdate = '2019-03-10'
    bsr = BullStockRatio('880883')
    bsr.update(end_date = cdate, num = 6000)
