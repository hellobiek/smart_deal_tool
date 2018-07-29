#coding=utf-8
import time
import json
import cmysql
import _pickle
import datetime
from datetime import datetime
import const as ct
import tushare as ts
import pandas as pd
import combination_info as cm_info 
from log import getLogger
from common import create_redis_obj, get_realtime_table_name

logger = getLogger(__name__)

class Combination:
    def __init__(self, dbinfo, code):
        self.redis = create_redis_obj()
        self.mysql_client = cmysql.CMySQL(dbinfo)
        self.code = code
        self.data_type_dict = {'D':"c%s_D" % code}
        self.realtime_table = "c%s_realtime" % code
        if not self.create(): raise Exception("create combination table failed")

    def __del__(self):
        self.redis.connection_pool.disconnect()

    def create_static(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(date varchar(10), code varchar(10), open float, high float, close float, low float, volume float)' % table_name
                if not self.mysql_client.create(sql, table_name): return False
        return True

    def create_realtime(self):
        sql = 'create table if not exists %s(name varchar(20), code varchar(10), price float, pre_close float, date varchar(25), time varchar(20), amount float, volume float)' % self.realtime_table
        return True if self.realtime_table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.realtime_table)

    def create(self):
        return self.create_static() and self.create_realtime()

    def get_code_list(self):
        contentStr = self.get('content')
        return contentStr.split(',')

    def compute(self, all_info, cdate = None):
        cdate = datetime.now().strftime('%Y-%m-%d') if cdate is None else cdate
        ctime = datetime.now().strftime('%H-%M-%S')
        code_list = self.get_code_list()
        df = pd.DataFrame()
        df = all_info[all_info.code.isin(code_list)]
        trading_df = df[df.volume != '0']
        num = len(trading_df)
        if 0 == num: return pd.DataFrame()
        _price = trading_df.price.astype(float).sum()/num
        _pre_close = trading_df.pre_close.astype(float).sum()/num
        _amount = trading_df.amount.astype(float).sum()/num
        _volume = trading_df.volume.astype(float).sum()/num
        data={'code':[self.code],'name':[self.get('name')],'date':[cdate],'time':[ctime],'price':[_price],'pre_close':[_pre_close],'amount':[_amount],'volume':[_volume]}
        return pd.DataFrame(data)

    def run(self, evt):
        all_info = evt.get()
        _new_data = self.compute(all_info)
        if not _new_data.empty:
            self.redis.set(get_realtime_table_name(self.code), _pickle.dumps(_new_data, 2))
            self.mysql_client.set(_new_data, self.realtime_table)

    def get(self, attribute):
        df_byte = self.redis.get(ct.COMBINATION_INFO)
        if df_byte is None: return None
        df = _pickle.loads(df_byte)
        return df.loc[df.code == self.code][attribute].values[0]
