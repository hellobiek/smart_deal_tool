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
from base.clog import getLogger
from ccalendar import CCalendar
from collections import OrderedDict
from common import create_redis_obj, get_tushare_client, smart_get
from base.cdate import get_day_nday_ago, delta_days, transfer_date_string_to_int, get_dates_array
class Margin(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, fpath = ct.TUSHAE_FILE):
        self.logger       = getLogger(__name__)
        self.crawler      = get_tushare_client(fpath = fpath)
        self.dbname       = self.get_dbname()
        self.redis        = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.cal_client   = CCalendar(dbinfo = dbinfo, redis_host = redis_host, without_init = True)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("init margin database failed")

    @staticmethod
    def get_dbname():
        return "margin"

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "%s_day_%s_%s" % (self.dbname, cdates[0], (int(cdates[1])-1)//3 + 1)

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             rzye float,\
                                             rzmre float,\
                                             rzche float,\
                                             rqye float,\
                                             rqyl float,\
                                             rqmcl float,\
                                             rqchl float,\
                                             rzrqye float,\
                                             PRIMARY KEY (date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get_k_data_in_range(self, start_date, end_date):
        ndays = delta_days(start_date, end_date)
        date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(date_dmy_format, periods=ndays, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        data_dict = OrderedDict()
        for _date in date_only_array:
            if self.cal_client.is_trading_day(_date):
                table_name = self.get_table_name(_date)
                if table_name not in data_dict: data_dict[table_name] = list()
                data_dict[table_name].append(str(_date))
        all_df = pd.DataFrame()
        for key in data_dict:
            table_list = sorted(data_dict[key], reverse=False)
            if len(table_list) == 1:
                df = self.get_data(table_list[0])
                if df is not None: all_df = all_df.append(df)
            else:
                start_date = table_list[0]
                end_date = table_list[len(table_list) - 1]
                df = self.get_data_between(start_date, end_date)
                if df is not None: all_df = all_df.append(df)
        return all_df

    def get_data_between(self, start_date, end_date):
        #start_date and end_date should be in the same table
        table_name = self.get_table_name(start_date)
        if not self.is_table_exists(table_name): return None
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        sql = "select * from %s where date=\"%s\"" % (self.get_table_name(cdate), cdate)
        return self.mysql_client.get(sql)

    def update(self, end_date = None, num = 10):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        succeed = True
        for mdate in date_array:
            if self.cal_client.is_trading_day(mdate):
                if mdate == end_date: continue
                if not self.set_data(mdate):
                    self.logger.error("%s set failed" % mdate)
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

        total_df = smart_get(self.crawler.margin, trade_date=transfer_date_string_to_int(cdate))
        if total_df is None:
            self.logger.error("crawel margin for %s failed" % cdate)
            return False

        total_df = total_df.rename(columns = {"trade_date": "date", "exchange_id": "code"})
        total_df['rqyl']  = 0
        total_df['rqchl'] = 0

        detail_df = smart_get(self.crawler.margin_detail, trade_date=transfer_date_string_to_int(cdate))
        if detail_df is None:
            self.logger.error("crawel detail margin for %s failed" % cdate)
            return False

        detail_df = detail_df.rename(columns = {"trade_date": "date", "ts_code": "code"})

        total_df = total_df.append(detail_df, sort = False)
        total_df['date'] = pd.to_datetime(total_df.date).dt.strftime("%Y-%m-%d")
        total_df = total_df.reset_index(drop = True)
        if self.mysql_client.set(total_df, table_name):
            time.sleep(1)
            return self.redis.sadd(table_name, cdate)
        return False

if __name__ == '__main__':
    ma = Margin()
    ma.update()
