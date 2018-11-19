#-*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import _pickle
import const as ct
import pandas as pd
from climit import CLimit
from cmysql import CMySQL
from log import getLogger
from datetime import datetime
from rstock import RIndexStock
from common import create_redis_obj
class Emotion:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo = dbinfo
        self.emotion_table = ct.EMOTION_TABLE
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client = CMySQL(self.dbinfo, iredis = self.redis)
        self.rstock_client = RIndexStock(dbinfo, redis_host)
        self.logger = getLogger(__name__)
        if not self.create(): raise Exception("create emotion table failed")

    def create(self):
        if self.emotion_table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, score float, PRIMARY KEY (date))' % self.emotion_table 
            if not self.mysql_client.create(sql, self.emotion_table): return False
        return True

    def get_score(self, cdate = None):
        if cdate is None:
            sql = "select * from %s" % self.emotion_table
        else:
            sql = "select * from %s where date=\"%s\"" %(self.emotion_table, cdate)
        return self.mysql_client.get(sql)

    def get_stock_data(self, cdate):

        df_byte = self.redis.get(ct.TODAY_ALL_STOCK)
        if df_byte is None: return None
        df = _pickle.loads(df_byte)
        return df.loc[df.date == date]

    def set_score(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        stock_info = self.rstock_client.get_data(cdate)
        limit_info = CLimit(self.dbinfo).get_data(cdate)
        if stock_info.empty or limit_info.empty:
            self.logger.error("get info failed")
            return False

        limit_up_list = limit_info[(limit_info.pchange > 0) & (limit_info.prange != 0)].reset_index(drop = True).code.tolist()
        limit_down_list = limit_info[limit_info.pchange < 0].reset_index(drop = True).code.tolist()
        limit_up_list.extend(limit_down_list)
        total = 0

        for _index, pchange in stock_info.pchange.iteritems():
            code = stock_info.loc[_index, 'code']
            if code in limit_up_list: 
                total += 2 * pchange
            else:
                total += pchange

        aver = total / len(stock_info)
        data = {'date':["%s" % datetime.now().strftime('%Y-%m-%d')], 'score':[aver]}

        df = pd.DataFrame.from_dict(data)
        return self.mysql_client.set(df, self.emotion_table)

if __name__ == '__main__':
    ce = Emotion()
    ce.set_score()
