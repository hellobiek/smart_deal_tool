# coding=utf-8
import _pickle
import cmysql
import const as ct
import pandas as pd
from log import getLogger
from pandas import DataFrame
from combination import Combination
from pytdx.reader import CustomerBlockReader
from common import trace_func, create_redis_obj, concurrent_run
# include index and concept in stock
class CombinationInfo:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.table = ct.COMBINATION_INFO_TABLE
        self.logger = getLogger(__name__)
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        if not self.init(): raise Exception("init combination table failed")
        #self.trigger = ct.SYNC_COMBINATION_2_REDIS
        #if not self.create(): raise Exception("create combination table failed")
        #if not self.register(): raise Exception("create combination trigger failed")

    def create(self):
        sql = 'create table if not exists %s(name varchar(50), code varchar(10), cType int, content varchar(20000), best varchar(1000), PRIMARY KEY (code))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('name', NEW.name, 'code', NEW.code, 'cType', NEW.cType, 'content', NEW.content, 'best', NEW.best));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    def init(self):
        new_df = DataFrame()
        new_self_defined_df = self.read_self_defined()
        new_self_defined_df['cType'] = ct.C_SELFD
        new_self_defined_df['best'] = '0'
        new_df = new_df.append(new_self_defined_df)
        new_df = new_df.reset_index(drop = True)
        return self.redis.set(ct.COMBINATION_INFO, _pickle.dumps(new_df, 2))

    def create_obj(self, code):
        try:
            Combination(code, should_create_db = True)
            return (code, True)
        except Exception as e:
            return (code, False)

    def update(self):
        if self.init():
            df = self.get(redis = self.redis)
            return concurrent_run(self.create_obj, df.code.tolist(), num = 10)
        return False

    def read_self_defined(self):
        df = CustomerBlockReader().get_df(ct.TONG_DA_XIN_SELF_PATH, 1)
        df = df[['block_type','blockname','code_list']]
        df.columns = ['code', 'name', 'content']
        return df
        
    @staticmethod
    def get(index_type = None, redis = None):
        redis = create_redis_obj() if redis is None else redis
        df_byte = redis.get(ct.COMBINATION_INFO) 
        if df_byte is None: return pd.DataFrame() 
        df = _pickle.loads(df_byte)
        if index_type is None: return df
        return df[[df.cType == index_type]]

if __name__ == '__main__':
    cm = CombinationInfo(ct.DB_INFO)
    cm.init()
