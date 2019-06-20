#coding=utf-8
import time
import _pickle
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
import ccalendar
from cmysql import CMySQL
from base.clog import getLogger
from common import trace_func, is_trading_time, create_redis_obj, df_delta
logger = getLogger(__name__)
class CHalted:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table):
        self.table = table
        self.redis = create_redis_obj()
        self.mysql_client = CMySQL(dbinfo)
        self.trigger = ct.SYNC_HALTED_2_REDIS
        if not self.create(): raise Exception("create chalted table failed")
        if not self.init(True): raise Exception("init chalted table failed")
        if not self.register(): raise Exception("create chalted trigger failed")

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(code varchar(6) not null, name varchar(20), stopTime varchar(20), stopDate varchar(20), showDate varchar(20), market varchar(20), stopReason varchar(100), PRIMARY KEY (code))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)
  
    @trace_func(log = logger)
    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('name', NEW.name, 'code', NEW.code, 'market', NEW.market, 'stopReason', NEW.stopReason, 'showDate', NEW.showDate, 'stopDate', NEW.stopDate, 'stopTime', NEW.stopTime));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    @trace_func(log = logger)
    def init(self, status):
        df = ts.get_halted()
        if df is None: return False
        if not self.mysql_client.set(df, self.table): return False
        if status: return self.redis.set(ct.HALTED_INFO, _pickle.dumps(df, 2))

    @trace_func(log = logger)
    def is_halted(self, code_id, _date = None):
        if _date is None: _date = datetime.now().strftime('%Y-%m-%d') 
        df = self.get(_date)
        if df is None: raise Exception("get chalted list failed") 
        if df.empty: return False
        return True if code_id in df['code'].tolist() else False

    @trace_func(log = logger)
    def get(self, _date = None):
        df_byte = self.redis.get(ct.HALTED_INFO)
        if df_byte is None: return pd.DataFrame()
        df = _pickle.loads(df_byte)
        if _date is None:
            return df
        else:
            return df.loc[df.date == _date]
