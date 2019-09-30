# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import pandas as pd
from cindex import CIndex
from algotrade.model import QModel
from base.cdate import get_dates_array
from algotrade.feed import dataFramefeed
from common import is_df_has_unexpected_data
from datamanager.bull_stock_ratio import BullStockRatio
class SilverLiningModel(QModel):
    def __init__(self, index_code = '000905', dbinfo = ct.DB_INFO, redis_host = None, cal_file_path = ct.CALENDAR_PATH, should_create_mysqldb = False):
        super(SilverLiningModel, self).__init__('silverlining', dbinfo, redis_host, cal_file_path)
        self.dbinfo = dbinfo
        self.index_code = index_code 
        self.redis_host = redis_host
        self.bull_ratio_client = BullStockRatio(self.index_code, dbinfo = self.dbinfo, redis_host = self.redis_host)
        if not self.create(should_create_mysqldb):
            raise Exception("create model {} table failed".format(self.code))

    def create(self, should_create_mysqldb):
        if should_create_mysqldb:
            #self.mysql_client.delete_db(self.dbname)
            return self.create_db(self.dbname) and self.create_order_table() and self.create_account_table() and self.create_position_table()
        return True

    def compute_stock_pool(self, mdate):
        df = self.bull_ratio_client.get_ratio(mdate)
        if not df.empty:
            df['code'] = self.index_code
            return df
        return pd.DataFrame()

    def create_table(self, table_name):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 code varchar(6) not null,\
                                                 ratio float not null,\
                                                 PRIMARY KEY(date, code))' % table_name 
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def generate_feed(self, start_date, end_date):
        all_df = pd.DataFrame()
        feed = dataFramefeed.Feed()
        date_array = get_dates_array(start_date, end_date, asending = True)
        is_first = True
        for mdate in date_array:
            if self.cal_client.is_trading_day(mdate):
                df = self.get_stock_pool(mdate)
                if not df.empty: all_df = all_df.append(df)
        codes = list(set(all_df.code.tolist()))
        for code in codes:
            data = CIndex(code).get_k_data()
            data = data[(data.date >= start_date) & (data.date <= end_date)]
            ratio_data = self.bull_ratio_client.get_ratio_between(start_date, end_date)
            ratio_data = ratio_data[(ratio_data.date >= start_date) & (ratio_data.date <= end_date)]
            data = data.sort_values(by=['date'], ascending = True)
            ratio_data = ratio_data.sort_values(by=['date'], ascending = True)
            data = data.reset_index(drop = True)
            ratio_data = ratio_data.reset_index(drop = True)
            data = data.set_index('date')
            ratio_data = ratio_data.set_index('date')
            data = pd.merge(data, ratio_data, left_index = True, right_index = True)
            if is_df_has_unexpected_data(data): return None, list()
            data.index = pd.to_datetime(data.index)
            data = data.dropna(how='any')
            feed.addBarsFromDataFrame(code, data)
        return feed, [self.index_code]

if __name__ == '__main__':
    start_date = '2017-09-25'
    end_date   = '2019-09-27'
    redis_host = "127.0.0.1"
    dbinfo = ct.OUT_DB_INFO
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    model = SilverLiningModel(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host, cal_file_path = cal_file_path, should_create_mysqldb = True)
    #df = model.compute_stock_pool('2018-10-01')
    #result = model.generate_stock_pool(start_date, end_date)
    xx, yy = model.generate_feed(start_date, end_date)
