#coding=utf-8
import time
import datetime
from datetime import datetime
import _pickle
import const as ct
import pandas as pd
from cindex import CIndex
from cmysql import CMySQL
from log import getLogger
from combination import Combination 
from common import create_redis_obj
from combination_info import CombinationInfo
logger = getLogger(__name__)
class CAnimation:
    def __init__(self, dbinfo):
        self.redis = create_redis_obj()
        self.mysql_client = CMySQL(dbinfo)
        self.table = ct.ANIMATION_INFO
        self.trigger = ct.SYNC_ANIMATION_2_REDIS
        if not self.create(): raise Exception("create animation table %s table failed" % self.table)
        if not self.register(): raise Exception("create animation trigger %s failed" % self.trigger)

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('date', NEW.date, 'time', NEW.time, 'price', NEW.price, 'volume', NEW.volume, 'amount', NEW.amount, 'name', NEW.name));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    def create(self):
        sql = 'create table if not exists %s(time varchar(10) not null, date varchar(10) not null, price float, volume float, amount float, name varchar(30) not null, PRIMARY KEY (date, time, name))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    @staticmethod
    def get_combination_dict():
        df = CombinationInfo.get()
        cdict = dict()
        for _index, code in df['code'].items():
            cdict[code] = df.loc[_index]['name']
        return cdict

    def collect(self):
        cdata = dict()
        cdata['name'] = list()
        cdata['price'] = list()
        cdata['volume'] = list()
        cdata['amount'] = list()
        cdict = self.get_combination_dict()
        if 0 == len(cdict): return False
        for code in ct.INDEX_DICT:
            df_byte = self.redis.get(CIndex.get_redis_name(CIndex.get_dbname(code)))
            if df_byte is None: continue
            df = _pickle.loads(df_byte)
            price = float(df.last_price.tolist()[0])
            p_volume = float(df.volume.tolist()[0])
            p_amount = float(df.turnover.tolist()[0])
            cdata['name'].append(ct.INDEX_DICT[code])
            cdata['price'].append(price)
            cdata['volume'].append(p_volume)
            cdata['amount'].append(p_amount)
        for code in cdict:
            key = cdict[code]
            df_byte = self.redis.get(Combination.get_redis_name(code))
            if df_byte is None: continue
            df = _pickle.loads(df_byte)
            price = float(df.price.tolist()[0])
            p_volume = float(df.volume.tolist()[0])
            p_amount = float(df.amount.tolist()[0])
            cdata['name'].append(key)
            cdata['price'].append(price)
            cdata['volume'].append(p_volume)
            cdata['amount'].append(p_amount)
        df = pd.DataFrame.from_dict(cdata)
        df['time'] = datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
        df['date'] = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        self.mysql_client.set(df, self.table)
        return True

if __name__ == '__main__':
    ani = CAnimation(ct.DB_INFO)
    ani.collect()
    #ani.redis.srem('all_tables', ani.table)
    #ani.redis.srem('all_triggers', ani.trigger)
    #ani.mysql_client.exec_sql("drop table animation;")
