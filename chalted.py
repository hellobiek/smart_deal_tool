#coding=utf-8
import time
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
import ccalendar
from cmysql import CMySQL
from cstock_info import CStockInfo
from common import trace_func, is_trading_time

logger = getLogger(__name__)

class CHalted:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table, stock_info_table, calendar_table):
        self.stock_info_client = CStockInfo(dbinfo, stock_info_table)
        self.cal_client = ccalendar.CCalendar(dbinfo, calendar_table)
        self.table = table
        self.mysql_client = CMySQL(dbinfo)
        if not self.create(): raise Exception("create chalted table:%s failed" % table)

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(code varchar(6),\
                                             name varchar(20),\
                                             market varchar(20),\
                                             date datetime,\
                                             reason varchar(100))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)
   
    @trace_func(log = logger)
    def choose(self, df):
        stocks_all = self.stock_info_client.get()
        return df[df['code'].isin(stocks_all['code'].tolist())]

    @trace_func(log = logger)
    def init(self):
        df = ts.get_halted()
        df = self.choose(df)
        old_df = self.get()
        if not old_df.empty:
            df = old_df.append(df)
            df = df.drop_duplicates(subset = ['date', 'code'])
        self.mysql_client.set(df, self.table)

    @trace_func(log = logger)
    def run(self, sleep_time):
        while True:
            if not self.cal_client.is_trading_day(): 
                time.sleep(ct.LONG_SLEEP_TIME)
            else:
                if is_trading_time(): 
                    self.init()
                time.sleep(sleep_time)

    @trace_func(log = logger)
    def is_halted(self, code_id, _date = None):
        if _date is None: _date = datetime.now().strftime('%Y-%m-%d') 
        df = self.get(_date)
        if df is not None:
            return True if code_id in df['code'].tolist() else False
        #else: get failed from mysql 

    @trace_func(log = logger)
    def get(self, _date = None):
        if _date is None:
            sql = "select * from %s" % self.table
        else:
            sql = "select * from %s where date = %s" % (self.table, _date)
        return self.mysql_client.get(sql)
