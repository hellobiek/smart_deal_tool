#coding=utf-8
from pandas import DataFrame
import time
import _pickle
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
from cmysql import CMySQL
from common import trace_func, create_redis_obj, df_delta

logger = getLogger(__name__)

class CDelisted:
    @trace_func(log = logger)
    def __init__(self, dbinfo):
        self.table = ct.DELISTED_INFO_TABLE
        self.trigger = ct.SYNC_DELISTED_2_REDIS
        self.mysql_client = CMySQL(dbinfo)
        self.redis = create_redis_obj()
        if not self.create(): raise Exception("create delisted table failed")
        if not self.init(True): raise Exception("init delisted table failed")
        if not self.register(): raise Exception("create delisted trigger failed")

    @trace_func(log = logger)
    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('name', NEW.name, 'code', NEW.code, 'oDate', NEW.oDate, 'tDate', NEW.tDate));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(code varchar(6) not null, name varchar(8), oDate varchar(10), tDate varchar(10), PRIMARY KEY (code))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)
    
    @trace_func(log = logger)
    def init(self, status):
        #get new delisted data info
        df_terminated = ts.get_terminated()
        if df_terminated is None:return False
        if not df_terminated.empty:
            df = df_terminated
        df_suspended = ts.get_suspended()
        if df_suspended is None:return False
        if not df_suspended.empty:
            df = df.append(df_suspended)
        if not df.empty:
            df = df.reset_index(drop = True)
        #get old delisted data info
        old_df_all = self.get()
        if not old_df_all.empty:
            df = df_delta(df, old_df_all, ['code'])
        if df.empty: return True
        res = self.mysql_client.set(df, self.table)
        if not res: return False
        if status:
            self.redis.set(ct.DELISTED_INFO, _pickle.dumps(df, 2))
        return True

    @trace_func(log = logger)
    def get_list(self):
        old_df_all = self.get()
        return old_df_all['code'].tolist()

    @trace_func(log = logger)
    def get(self, code = None, column = None):
        df_byte = self.redis.get(ct.DELISTED_INFO)
        if df_byte is None: return pd.DataFrame()
        df = _pickle.loads(df_byte)
        if code is None: return df
        if column is None:
            return df.loc[df.code == code]
        else:
            return df.loc[df.code == code][column][0]

    @trace_func(log = logger)
    def is_dlisted(self, code_id, _date):
        if code_id not in self.get_list(): return False
        terminatedTime = self.get(code = code_id, column = 'tDate')
        if terminatedTime == '-': return True
        if terminatedTime:
            t = time.strptime(terminatedTime, "%Y-%m-%d")
            y,m,d = t[0:3]
            terminatedTime = datetime(y,m,d)
            return (datetime.strptime(_date, "%Y-%m-%d") - terminatedTime).days > 0
        return False

if __name__ == '__main__':
    cdlist = CDelisted(ct.DB_INFO)
    print(cdlist.init(False))
