#coding=utf-8
import time
import datetime
import const as ct
import numpy as np
import pandas as pd
from functools import partial
from datetime import datetime
from base.clog import getLogger
from cmysql import CMySQL
from cstock import CStock
from ccalendar import CCalendar
from cstock_info import CStockInfo
from collections import OrderedDict
from common import delta_days, create_redis_obj, get_day_nday_ago, get_dates_array, queue_process_concurrent_run
class RIndexStock:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.dbname = self.get_dbname()
        self.redis_host = redis_host
        self.logger = getLogger(__name__)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.get_dbname()): raise Exception("init rstock database failed")

    @staticmethod
    def get_dbname():
        return ct.RINDEX_STOCK_INFO_DB

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "%s_day_%s_%s" % (self.get_dbname(), cdates[0], (int(cdates[1])-1)//3 + 1)

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(str(tdate, encoding = "utf8") for tdate in self.redis.smembers(table_name))
        return False

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return table_name in set(str(table, encoding = "utf8") for table in self.redis.smembers(self.dbname))
        return False

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             open float,\
                                             high float,\
                                             close float,\
                                             preclose float,\
                                             low float,\
                                             volume float,\
                                             amount float,\
                                             outstanding float,\
                                             totals float,\
                                             adj float,\
                                             aprice float,\
                                             pchange float,\
                                             turnover float,\
                                             sai float,\
                                             sri float,\
                                             uprice float,\
                                             sprice float,\
                                             mprice float,\
                                             lprice float,\
                                             ppercent float,\
                                             npercent float,\
                                             base float,\
                                             ibase bigint,\
                                             breakup int,\
                                             ibreakup bigint,\
                                             pday int,\
                                             profit float,\
                                             gamekline float,\
                                             PRIMARY KEY (date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get_k_data_in_range(self, start_date, end_date):
        ndays = delta_days(start_date, end_date)
        date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(date_dmy_format, periods=ndays, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        data_dict = OrderedDict()
        for _date in date_only_array:
            if CCalendar.is_trading_day(_date, redis = self.redis):
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
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (self.get_table_name(start_date), start_date, end_date)
        return self.mysql_client.get(sql)

    def get_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        sql = "select * from %s where date=\"%s\"" % (self.get_table_name(cdate), cdate)
        return self.mysql_client.get(sql)

    def get_stock_data(self, cdate, code):
        return (code, CStock(code).get_k_data(cdate))

    def generate_all_data_1(self, cdate, black_list = list()):
        failed_list = CStockInfo(redis_host = self.redis_host).get(redis = self.redis).code.tolist()
        if len(black_list) > 0: failed_list = list(set(failed_list).difference(set(black_list)))
        cfunc = partial(self.get_stock_data, cdate)
        return queue_process_concurrent_run(cfunc, failed_list, redis_client = self.redis)

    def generate_all_data(self, cdate, black_list = ct.BLACK_LIST):
        from gevent.pool import Pool
        obj_pool = Pool(500)
        failed_list = CStockInfo(redis_host = self.redis_host).get(redis = self.redis).code.tolist()
        if len(black_list) > 0:
            failed_list = list(set(failed_list).difference(set(black_list)))
        all_df = pd.DataFrame()
        last_length = len(failed_list)
        cfunc = partial(self.get_stock_data, cdate)
        while last_length > 0:
            self.logger.info("all stock list:%s, cdate:%s", len(failed_list), cdate)
            for code_data in obj_pool.imap_unordered(cfunc, failed_list):
                if code_data[1] is not None:
                    tem_df = code_data[1]
                    tem_df['code'] = code_data[0]
                    all_df = all_df.append(tem_df)
                    failed_list.remove(code_data[0])
            if len(failed_list) != last_length:
                last_length = len(failed_list)
                if last_length > 0: time.sleep(300)
        obj_pool.join(timeout = 5)
        obj_pool.kill()
        all_df = all_df.drop_duplicates()
        all_df = all_df.sort_values(by = 'date', ascending= True)
        all_df = all_df.reset_index(drop = True)
        return all_df

    def update(self, end_date = datetime.now().strftime('%Y-%m-%d'), num = 30):
        #if end_date == datetime.now().strftime('%Y-%m-%d'): end_date = get_day_nday_ago(end_date, num = 1, dformat = "%Y-%m-%d")
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        succeed = True
        count = 0
        for mdate in date_array:
            count += 1
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                if not self.set_day_data(mdate):
                    self.logger.error("set %s data for rstock failed" % mdate)
                    succeed = False
        return succeed

    def set_day_data(self, cdate):
        table_name = self.get_table_name(cdate)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                self.logger.error("create tick table failed")
                return False
            self.redis.sadd(self.dbname, table_name)
        if self.is_date_exists(table_name, cdate): 
            self.logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return True
        df = self.generate_all_data(cdate)
        if df is None: return False
        if self.mysql_client.set(df, table_name):
            self.redis.sadd(table_name, cdate)
            return True
        return False

if __name__ == '__main__':
    ris = RIndexStock(ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    ris.update(end_date = '2019-03-16', num = 20000)
