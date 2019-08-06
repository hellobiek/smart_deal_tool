#coding=utf-8
import time
import _pickle
import datetime
import const as ct
import numpy as np
import pandas as pd
from cmysql import CMySQL
from cindex import CIndex
from gevent.pool import Pool
from datetime import datetime
from functools import partial
from ccalendar import CCalendar
from base.clog import getLogger
from base.cdate import delta_days
from common import create_redis_obj
from collections import OrderedDict
from industry_info import IndustryInfo
from base.cdate import get_day_nday_ago, get_dates_array
RINDEX_INDUSTRY_INFO_DB = "rindex_industry"
class RIndexIndustryInfo:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.dbname = self.get_dbname()
        self.logger = getLogger(__name__)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.get_dbname()): raise Exception("init rindex stock database failed")

    @staticmethod
    def get_dbname():
        return RINDEX_INDUSTRY_INFO_DB

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "rindustry_day_%s_%s" % (cdates[0], (int(cdates[1])-1)//3 + 1)

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return self.redis.sismember(self.dbname, table_name)
        return False

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             open float,\
                                             high float,\
                                             close float,\
                                             preclose float,\
                                             low float,\
                                             volume bigint,\
                                             amount float,\
                                             preamount float,\
                                             pchange float,\
                                             mchange float,\
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
        #start_date and end_date shoulw be in the same table
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (self.get_table_name(start_date), start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, cdate):
        cdate = datetime.now().strftime('%Y-%m-%d') if cdate is None else cdate
        sql = "select * from %s where date=\"%s\"" % (self.get_table_name(cdate), cdate)
        return self.mysql_client.get(sql)

    def get_industry_data(self, cdate, code):
        return (code, CIndex(code).get_k_data(cdate))

    def generate_data(self, cdate):
        good_list = list()
        obj_pool = Pool(500)
        all_df = pd.DataFrame()
        industry_info = IndustryInfo.get(self.redis)
        failed_list = industry_info.code.tolist()
        cfunc = partial(self.get_industry_data, cdate)
        failed_count = 0
        while len(failed_list) > 0:
            is_failed = False
            self.logger.debug("restart failed ip len(%s)" % len(failed_list))
            for code_data in obj_pool.imap_unordered(cfunc, failed_list):
                if code_data[1] is not None:
                    tem_df = code_data[1]
                    tem_df['code'] = code_data[0]
                    all_df = all_df.append(tem_df)
                    failed_list.remove(code_data[0])
                else:
                    is_failed = True
            if is_failed:
                failed_count += 1
                if failed_count > 10: 
                    self.logger.info("%s rindustry init failed" % failed_list)
                    return pd.DataFrame()
                time.sleep(10)
        obj_pool.join(timeout = 5)
        obj_pool.kill()
        self.mysql_client.changedb(self.get_dbname())
        if all_df.empty: return all_df
        all_df = all_df.reset_index(drop = True)
        return all_df

    def set_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        if not CCalendar.is_trading_day(cdate, redis = self.redis): return False
        table_name = self.get_table_name(cdate)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                self.logger.error("create rindex table failed")
                return False
            self.redis.sadd(self.dbname, table_name)
        if self.is_date_exists(table_name, cdate): 
            self.logger.debug("existed rindex table:%s, date:%s" % (table_name, cdate))
            return True
        df = self.generate_data(cdate)
        if df.empty: return False
        self.redis.set(ct.TODAY_ALL_INDUSTRY, _pickle.dumps(df, 2))
        if self.mysql_client.set(df, table_name):
            return self.redis.sadd(table_name, cdate)
        return False

    def update(self, end_date = None, num = 10):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        #if end_date == datetime.now().strftime('%Y-%m-%d'): end_date = get_day_nday_ago(end_date, num = 1, dformat = "%Y-%m-%d")
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        succeed = True
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                if not self.set_data(mdate):
                    self.logger.error("%s rindustry set failed" % mdate)
                    succeed = False
        return succeed
