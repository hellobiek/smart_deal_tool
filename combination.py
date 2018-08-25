#coding=utf-8
import time
import json
import cmysql
import _pickle
import datetime
from datetime import datetime
from cinfluxdb import CInflux  
import const as ct
import tushare as ts
import pandas as pd
from log import getLogger
from cstock import CStock
from common import create_redis_obj
logger = getLogger(__name__)
class Combination:
    def __init__(self, dbinfo, code):
        self.code = code
        self.dbname = self.get_dbname(code)
        self.redis = create_redis_obj()
        self.data_type_dict = {9:"day"}
        self.influx_client = CInflux(ct.IN_DB_INFO, self.dbname)
        self.mysql_client = cmysql.CMySQL(dbinfo, self.dbname)
        if not self.create(): raise Exception("%s create combination table failed" % code)

    @staticmethod
    def get_dbname(code):
        return "c%s" % code

    @staticmethod
    def get_redis_name(code):
        return "realtime_%s" % code

    def create_mysql_table(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(date varchar(10), open float, high float, close float, low float, volume float)' % table_name
                if not self.mysql_client.create(sql, table_name): return False
        return True

    def create_influx_db(self):
        self.influx_client.create()

    def create(self):
        self.create_influx_db()
        return self.create_mysql_table()

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
