#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import const as ct
import numpy as np
import pandas as pd
from cmysql import CMySQL
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from collections import OrderedDict
from common import create_redis_obj
from datamanager.hk_crawl import MCrawl
from base.cdate import get_day_nday_ago, get_dates_array, delta_days
class StockConnect(object):
    def __init__(self, market_from = ct.SH_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL, dbinfo = ct.DB_INFO, redis_host = None):
        self.market_from  = market_from
        self.market_to    = market_to
        self.balcklist    = None
        self.crawler      = None
        self.mysql_client = None
        self.dbinfo       = dbinfo
        self.logger       = getLogger(__name__)
        self.dbname       = self.get_dbname(market_from, market_to)
        self.redis        = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(self.dbinfo, self.dbname, iredis = self.redis)

    def set_market(self, market_from, market_to):
        self.market_from  = market_from
        self.market_to    = market_to
        self.balcklist    = ["2018-10-17", "2018-09-25", "2018-07-02", "2018-05-22", "2018-04-02", "2018-03-30"] if market_from in [ct.SH_MARKET_SYMBOL, ct.SZ_MARKET_SYMBOL] else list()
        self.dbname       = self.get_dbname(market_from, market_to)
        self.crawler      = MCrawl(market_from)
        self.mysql_client = CMySQL(self.dbinfo, self.dbname, iredis = self.redis)
        return False if not self.mysql_client.create_db(self.dbname) else True

    def quit(self):
        self.crawler.quit()

    def close(self):
        self.crawler.close()

    @staticmethod
    def get_dbname(mfrom, mto):
        return "%s2%s" % (mfrom, mto)

    def get_table_name(self, cdate, dtype = None):
        if dtype == ct.HGT_CAPITAL:
            table_name = self.get_capital_table_name()
        elif dtype == ct.HGT_TOPTEN:
            table_name = self.get_topten_table_name()
        else:
            table_name = self.get_stock_table_name(cdate)
        return table_name

    def get_stock_table_name(self, cdate):
        cdates = cdate.split('-')
        return "%s_stock_day_%s_%s" % (self.dbname, cdates[0], (int(cdates[1])-1)//3 + 1)

    def get_capital_table_name(self):
        return "%s_capital_overview" % self.dbname

    def get_topten_table_name(self):
        return "%s_topten" % self.dbname

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             name varchar(90),\
                                             volume int,\
                                             percent float,\
                                             PRIMARY KEY (date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get_k_data_in_range(self, start_date, end_date, dtype = None):
        ndays = delta_days(start_date, end_date)
        date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(date_dmy_format, periods=ndays, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        data_dict = OrderedDict()
        for mdate in date_only_array:
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                table_name = self.get_table_name(mdate, dtype)
                if table_name not in data_dict: data_dict[table_name] = list()
                data_dict[table_name].append(str(mdate))
        all_df = pd.DataFrame()
        for key in data_dict:
            table_list = sorted(data_dict[key], reverse=False)
            if len(table_list) == 1:
                df = self.get_k_data(table_list[0], dtype)
                if df is not None: all_df = all_df.append(df)
            else:
                start_date = table_list[0]
                end_date = table_list[len(table_list) - 1]
                df = self.get_data_between(start_date, end_date, dtype)
                if df is not None: all_df = all_df.append(df)
        return all_df

    def get_data_between(self, start_date, end_date, dtype = None):
        #start_date and end_date should be in the same table
        table_name = self.get_table_name(start_date, dtype)
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, cdate = None, dtype = None):
        table_name = self.get_table_name(cdate, dtype)
        if cdate is None:
            sql = "select * from %s" % table_name
        else:
            sql = "select * from %s where date=\"%s\"" % (table_name, cdate)
        return self.mysql_client.get(sql)

    def update(self, end_date = None, num = 14):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        succeed = True
        for mdate in get_dates_array(start_date, end_date):
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                if mdate == end_date or mdate in self.balcklist: continue
                if not self.set_data(mdate):
                    succeed = False
        return succeed

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return self.redis.sismember(self.dbname, table_name)
        return False

    def set_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        table_name = self.get_table_name(cdate)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                self.logger.error("create tick table failed")
                return False
            self.redis.sadd(self.dbname, table_name)

        if self.is_date_exists(table_name, cdate): 
            self.logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return True

        ret, df = self.crawler.crawl(cdate)
        if ret != 0: return False
        if df.empty: return True
        df = df.reset_index(drop = True)
        df['date'] = cdate
        if self.mysql_client.set(df, table_name):
            return self.redis.sadd(table_name, cdate)
        return False

if __name__ == '__main__':
    sc = StockConnect(market_from = "SZ", market_to = "HK")
    sc.set_market(market_from = "SZ", market_to = "HK")
    sc.update()
