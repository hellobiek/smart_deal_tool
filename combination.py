#coding=utf-8
import time
import json
import cmysql
import _pickle
import datetime
from datetime import datetime
from cinfluxdb import CInflux  
import const as ct
import pandas as pd
from log import getLogger
from cstock import CStock
from common import create_redis_obj
logger = getLogger(__name__)
class Combination:
    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None):
        self.code = code
        self.dbname = self.get_dbname(code)
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.data_type_dict = {9:"day"}
        self.influx_client = CInflux(ct.IN_DB_INFO, self.dbname, iredis = self.redis)
        self.mysql_client = cmysql.CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.create(): raise Exception("%s create combination table failed" % code)

    @staticmethod
    def get_dbname(code):
        return "c%s" % code

    @staticmethod
    def get_redis_name(code):
        return "realtime_i%s" % code

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
            return table_name in set(str(table, encoding = "utf8") for table in self.redis.smembers(self.dbname))
        return False

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(str(tdate, encoding = "utf8") for tdate in self.redis.smembers(table_name))
        return False

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
