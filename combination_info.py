# coding=utf-8
import json
import _pickle
import cmysql
import const as ct
import tushare as ts
import pandas as pd
from log import getLogger
from pandas import DataFrame
from pytdx.reader import CustomerBlockReader
from common import trace_func, create_redis_obj, df_delta

logger = getLogger(__name__)

# include index and concept in stock
class CombinationInfo:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        self.table = table_name
        self.redis = create_redis_obj()
        if not self.init(): raise Exception("init combination table failed")
        #self.trigger = ct.SYNC_COMBINATION_2_REDIS
        #self.mysql_client = cmysql.CMySQL(dbinfo)
        #if not self.create(): raise Exception("create combination table failed")
        #if not self.register(): raise Exception("create combination trigger failed")

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(name varchar(50), code varchar(10), cType int, content varchar(20000), best varchar(1000))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    @trace_func(log = logger)
    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('name', NEW.name, 'code', NEW.code, 'cType', NEW.cType, 'content', NEW.content, 'best', NEW.best));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    @trace_func(log = logger)
    def init(self):
        new_df = DataFrame()
        new_self_defined_df = self.read_self_defined()
        new_self_defined_df['cType'] = ct.C_SELFD
        new_self_defined_df['best'] = '0'
        new_df = new_df.append(new_self_defined_df)
        new_df = new_df.reset_index(drop = True)
        return self.redis.set(ct.COMBINATION_INFO, _pickle.dumps(new_df, 2))

    @trace_func(log = logger)
    def read_self_defined(self):
        df = CustomerBlockReader().get_df(ct.TONG_DA_XIN_SELF_PATH, 1)
        df = df[['block_type','blockname','code_list']]
        df.columns = ['code', 'name', 'content']
        return df
        
    @trace_func(log = logger)
    def get(self, index_type = None):
        df_byte = self.redis.get(ct.COMBINATION_INFO) 
        if df_byte is None: return pd.DataFrame() 
        df = _pickle.loads(df_byte)
        if index_type is None: return df
        return df[[df.cType == index_type]]

if __name__ == '__main__':
    cm = CombinationInfo(ct.DB_INFO, ct.COMBINATION_INFO_TABLE)
