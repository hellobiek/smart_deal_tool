#coding=utf-8
import time
import cmysql
import _pickle
import datetime
import const as ct
import pandas as pd
from cstock import CStock
from datetime import datetime
from cinfluxdb import CInflux
from base.cobj import CMysqlObj
class Combination(CMysqlObj):
    def __init__(self, code, should_create_db = False, dbinfo = ct.DB_INFO, redis_host = None):
        super(Combination, self).__init__(code, self.get_dbname(code), dbinfo, redis_host)
        self.code = code
        self.influx_client = CInflux(ct.IN_DB_INFO, self.dbname, iredis = self.redis)
        if should_create_db:
            if not self.create(): raise Exception("%s create combination table failed" % code)

    @staticmethod
    def get_dbname(code):
        return "c%s" % code

    @staticmethod
    def get_redis_name(code):
        return "realtime_c%s" % code

    def create_influx_db(self):
        return self.influx_client.create()

    def create(self):
        return self.create_influx_db()

    def get_code_list(self):
        contentStr = self.get('content')
        return contentStr.split(',')

    def compute(self):
        code_list = self.get_code_list()
        df = pd.DataFrame()
        for code in code_list:
            df_byte = self.redis.get(CStock.get_redis_name(code))
            if df_byte is None: continue
            df = df.append(_pickle.loads(df_byte))
        num = len(df)
        if 0 == num: return pd.DataFrame()
        _price = df.price.astype(float).sum()/num
        _volume = df.volume.astype(float).sum()/num
        _amount = df.turnover.astype(float).sum()/num
        ctime = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        data={'code':[self.code],'name':[self.get('name')],'time':[ctime],'price':[_price],'amount':[_amount],'volume':[_volume]}
        df = pd.DataFrame(data)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        return df

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return table_name in set(table.decode() for table in self.redis.smembers(self.dbname))
        return False

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(tdate.decode() for tdate in self.redis.smembers(table_name))
        return False

    def get_existed_keys_list(self, table_name):
        if self.redis.exists(table_name):
            return list(tdate.decode() for tdate in self.redis.smembers(table_name))
        return list()

    def run(self):
        _new_data = self.compute()
        if not _new_data.empty:
            self.redis.set(self.get_redis_name(self.code), _pickle.dumps(_new_data, 2))
            self.influx_client.set(_new_data)

    def get(self, attribute):
        df_byte = self.redis.get(ct.COMBINATION_INFO)
        if df_byte is None: return None
        df = _pickle.loads(df_byte)
        return df.loc[df.code == self.code][attribute].values[0]
