#coding=utf-8
import time
import cmysql
import _pickle
import datetime
from gevent.pool import Pool
from datetime import datetime
from functools import partial
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from common import delta_days, create_redis_obj
from log import getLogger
from cstock import CStock
from cstock_info import CStockInfo
from ccalendar import CCalendar
from collections import OrderedDict
logger = getLogger(__name__)

class RIndexStock:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.dbname = self.get_dbname()
        self.mysql_client = cmysql.CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.get_dbname()): raise Exception("init rindex stock database failed")

    @staticmethod
    def get_dbname():
        return ct.RINDEX_STOCK_INFO_DB

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "day_%s_%s" % (cdates[0], (int(cdates[1])-1)//3 + 1)

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
                                             name varchar(10),\
                                             changepercent float,\
                                             trade float,\
                                             open float,\
                                             high float,\
                                             low float,\
                                             settlement float,\
                                             volume float,\
                                             turnoverratio float,\
                                             amount float,\
                                             per float,\
                                             pb float,\
                                             mktcap float,\
                                             nmc float,\
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

    def get_stock_data(self, date, code):
        sql = "select * from day where date=\"%s\"" % date
        self.mysql_client.changedb(CStock.get_dbname(code))
        return (code, self.mysql_client.get(sql))

    def generate_all_data(self, cdate):
        good_list = list()
        obj_pool = Pool(500)
        all_df = pd.DataFrame()
        stock_info = CStockInfo.get()
        failed_list = stock_info.code.tolist()
        cfunc = partial(self.get_stock_data, cdate)
        while len(failed_list) > 0:
            logger.info("restart failed ip len(%s)" % len(failed_list))
            for code_data in obj_pool.imap_unordered(cfunc, failed_list):
                if code_data[1] is not None:
                    tem_df = code_data[1]
                    tem_df['code'] = code_data[0]
                    all_df = all_df.append(tem_df)
                    failed_list.remove(code_data[0])
        obj_pool.join(timeout = 5)
        obj_pool.kill()
        self.mysql_client.changedb(self.get_dbname())
        all_df = all_df.reset_index(drop = True)
        return all_df

    def set_data(self):
        cdate = datetime.now().strftime('%Y-%m-%d')
        table_name = self.get_table_name(cdate)
        self.create_table(table_name)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                logger.error("create tick table failed")
                return False
            self.redis.sadd(self.dbname, table_name)
        if self.is_date_exists(table_name, cdate): 
            logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return False
        df = self.generate_all_data(cdate)
        self.redis.set(ct.TODAY_ALL_STOCK, _pickle.dumps(df, 2))
        if self.mysql_client.set(df, table_name):
            self.redis.sadd(table_name, cdate)
            return True
        return False

if __name__ == '__main__':
    #start_date = '2018-03-25'
    #end_date = '2018-04-05'
    ris = RIndexStock(ct.DB_INFO, redis_host = '127.0.0.1')
    ris.set_data()
