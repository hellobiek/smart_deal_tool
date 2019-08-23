# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import pandas as pd
from cstock import CStock
from rstock import RIndexStock
from ccalendar import CCalendar
from base.cobj import CMysqlObj
from cstock_info import CStockInfo
from cpython.cval import CValuation
from algotrade.technical.kdj import kdj
from algotrade.feed import dataFramefeed
from datetime import datetime, timedelta
from common import is_df_has_unexpected_data
from base.cdate import transfer_date_string_to_int, get_dates_array
class FollowTrendModel(CMysqlObj):
    def __init__(self, code, valuation_path = ct.VALUATION_PATH, bonus_path = ct.BONUS_PATH, 
                stocks_dir = ct.STOCKS_DIR, base_stock_path = ct.BASE_STOCK_PATH,
                report_dir = ct.REPORT_DIR, report_publish_dir = ct.REPORT_PUBLISH_DIR,
                pledge_file_dir = ct.PLEDGE_FILE_DIR, rvaluation_dir = ct.RVALUATION_DIR,
                cal_file_path = ct.CALENDAR_PATH, 
                dbinfo = ct.DB_INFO, should_create_mysqldb = False, redis_host = None):
        super(FollowTrendModel, self).__init__(code, self.get_dbname(), dbinfo, redis_host)
        self.cal_client = CCalendar(dbinfo = dbinfo, redis_host = redis_host, filepath = cal_file_path)
        self.val_client = CValuation(valuation_path, bonus_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir)
        self.rindex_client = RIndexStock(dbinfo, redis_host)
        self.stock_info_client = CStockInfo(dbinfo, redis_host, stocks_dir, base_stock_path)
        if not self.create(should_create_mysqldb):
            raise Exception("create model {} table failed".format(self.code))

    def create(self, should_create_mysqldb):
        return self.create_db(self.dbname) if should_create_mysqldb else True

    def create_table(self, table_name):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 code varchar(6) not null,\
                                                 name varchar(150) not null,\
                                                 industry varchar(150) not null,\
                                                 PRIMARY KEY(date, code))' % table_name 
            if not self.mysql_client.create(sql, table_name): return False
        return True

    @staticmethod
    def get_dbname():
        return "model"

    def get_hist_val(self, black_set, white_set, code):
        if code in white_set:
            return 1
        elif code in black_set:
            return -1
        else:
            return 0

    def get_min_val_in_range(self, dtype, code):
        vdf = self.val_client.get_horizontal_data(code)
        vdf = vdf[(vdf['date'] - 1231) % 10000 == 0]
        vdf = vdf[-5:]
        return vdf[dtype].median()

    def compute_stock_pool(self, mdate):
        df = self.rindex_client.get_data(mdate)
        df['mv'] = df['totals'] * df['close'] / 100000000
        df['hlzh'] = df['ppercent'] - df['npercent']
        df = df[df.pday > 100]
        df = df[(df.mv > 100) & (df.mv < 2500)]
        df = df[df.hlzh > 20]
        df = df[(df.profit > 2) & (df.profit < 6.5)]
        #黑名单
        black_set = set(ct.BLACK_DICT.keys())
        white_set = set(ct.WHITE_DICT.keys())
        if len(black_set.intersection(white_set)) > 0: raise Exception("black and white has intersection.")
        df['history'] = df.apply(lambda row: self.get_hist_val(black_set, white_set, row['code']), axis = 1)
        df = df[df['history'] > -1]
        #添加上市时间和行业信息
        base_df = self.stock_info_client.get(redis = self.stock_info_client.redis)
        base_df = base_df[['code', 'name', 'timeToMarket', 'industry']]
        df = pd.merge(df, base_df, how='inner', on=['code'])
        start_time = int((datetime.now() - timedelta(days = 1825)).strftime('%Y%m%d'))
        df = df[(df['timeToMarket'] < start_time) | df.code.isin(list(ct.WHITE_DICT.keys()))]
        #不买包含ST的股票
        df = df[~df.name.str.contains("ST")]
        #质押率
        pledge_info = self.val_client.get_stock_pledge_info()
        pledge_info = pledge_info[['code', 'pledge_rate']]
        df = pd.merge(df, pledge_info, how='left', on=['code'])
        df = df.fillna(value = {'pledge_rate': 0})
        df = df[df['pledge_rate'] < 50]
        #ROE中位数
        df['min_roa'] = df.apply(lambda row: self.get_min_val_in_range('roa', row['code']), axis = 1)
        df = df[df['min_roa'] > 7]
        #基本面信息
        self.val_client.update_vertical_data(df, ['goodwill', 'ta'], transfer_date_string_to_int(mdate))
        df['gwr'] = 100 * df['goodwill'] / df['ta']
        df = df[df['gwr'] < 30]
        df = df.dropna()
        df = df.reset_index(drop = True)
        df = df[['date', 'code', 'name', 'industry']]
        return df

    def get_table_name(self, mdate):
        mdates = mdate.split('-')
        return "{}_{}".format(self.code, mdates[0])

    def generate_feed(self, start_date, end_date):
        all_df = pd.DataFrame()
        feed = dataFramefeed.Feed()
        date_array = get_dates_array(start_date, end_date, asending = True)
        is_first = True
        code_list = list()
        for mdate in date_array:
            if self.cal_client.is_trading_day(mdate, redis = self.cal_client.redis):
                df = self.compute_stock_pool(mdate)
                if is_first:
                   code_list = df.code.tolist()
                   is_first = False
                all_df = all_df.append(df)
        codes = list(set(all_df.code.tolist()))
        all_df = all_df.set_index('date')
        all_df.index = pd.to_datetime(all_df.index)
        for code in codes:
            data = CStock(code).get_k_data()
            data = kdj(data)
            data = data[(data.date >= start_date) & (data.date <= end_date)]
            data = data.sort_values(by=['date'], ascending = True)
            data = data.reset_index(drop = True)
            data = data.set_index('date')
            if is_df_has_unexpected_data(data): return None, None, None
            data.index = pd.to_datetime(data.index)
            data = data.dropna(how='any')
            feed.addBarsFromDataFrame(code, data)
        return all_df, feed, code_list

    def set_data(self, mdate):
        table_name = self.get_table_name(mdate)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                logger.error("create chip table:{} failed".format(table_name))
                return (myear, False)

        if self.is_date_exists(table_name, mdate):
            logger.debug("existed data for code:{}, date:{}".format(self.code, mdate))
            return True

        df = self.compute_stock_pool(mdate)

        if self.mysql_client.set(df, table_name):
            return self.redis.sadd(table_name, mdate)
        return False

if __name__ == '__main__':
    start_date = '2019-06-01'
    end_date   = '2019-08-16'
    redis_host = "127.0.0.1"
    dbinfo = ct.OUT_DB_INFO
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    ftm = FollowTrendModel('follow_trend', valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo = dbinfo, redis_host = redis_host)
    df, feed, code_list = ftm.generate_feed(start_date, end_date)
