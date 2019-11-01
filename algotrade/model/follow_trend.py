# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import pandas as pd
from cstock import CStock
from itertools import chain
from rstock import RIndexStock
from ccalendar import CCalendar
from base.cobj import CMysqlObj
from base.clog import getLogger 
from algotrade.model import QModel
from cstock_info import CStockInfo
from cpython.cval import CValuation
from algotrade.technical.kdj import kdj
from algotrade.technical.boll import boll
from algotrade.feed import dataFramefeed
from datetime import datetime, timedelta
from combination_info import CombinationInfo
from common import is_df_has_unexpected_data
from base.cdate import transfer_date_string_to_int, get_dates_array
EXCLUDE_LIST = ['601398', '601288', '601988', '000951', '600886']
class FollowTrendModel(QModel):
    def __init__(self, valuation_path = ct.VALUATION_PATH,
                bonus_path = ct.BONUS_PATH, stocks_dir = ct.STOCKS_DIR, 
                base_stock_path = ct.BASE_STOCK_PATH, report_dir = ct.REPORT_DIR,
                report_publish_dir = ct.REPORT_PUBLISH_DIR, pledge_file_dir = ct.PLEDGE_FILE_DIR,
                rvaluation_dir = ct.RVALUATION_DIR, cal_file_path = ct.CALENDAR_PATH,
                dbinfo = ct.DB_INFO, redis_host = None, should_create_mysqldb = False):
        super(FollowTrendModel, self).__init__('follow_trend', dbinfo, redis_host, cal_file_path)
        self.rindex_client = RIndexStock(dbinfo, redis_host)
        self.comb_info_client = CombinationInfo(dbinfo, redis_host, needUpdate = False)
        self.stock_info_client = CStockInfo(dbinfo, redis_host, stocks_dir, base_stock_path)
        self.val_client = CValuation(valuation_path, bonus_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir)
        self.gwr = 35
        self.pday = 0
        self.hlzh = 50
        self.min_hlzh = 15
        self.min_ppercent = 50
        self.profit = 0
        self.max_profit = 7
        self.min_roe = 6
        self.min_market_val = 100
        self.pledge_rate = 50
        self.existed_days = 1825
        if not self.create(should_create_mysqldb):
            raise Exception("create model {} table failed".format(self.code))

    def create(self, should_create_mysqldb):
        if should_create_mysqldb:
            return self.create_db(self.dbname) and self.create_order_table() and self.create_account_table() and self.create_position_table()
        return True

    def create_table(self, table_name):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 code varchar(6) not null,\
                                                 name varchar(150) not null,\
                                                 sw_industry varchar(150) not null,\
                                                 leader boolean,\
                                                 days int,\
                                                 timeToMarket int,\
                                                 PRIMARY KEY(date, code))' % table_name
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def get_hist_val(self, black_set, white_set, code):
        if code in white_set:
            return 1
        elif code in black_set:
            return -1
        else:
            return 0

    def get_val_in_range(self, dtype, code, vtype = 'mid'):
        vdf = self.val_client.get_horizontal_data(code)
        vdf = vdf[(vdf['date'] - 1231) % 10000 == 0]
        vdf = vdf[-3:]
        if vtype == 'mid':
            return vdf[dtype].median()
        else:
            return vdf[dtype].min()

    def get_max_profit(self, code, mdate):
        data = CStock(code).get_k_data()
        mdata = data.loc[data.date == mdate]
        if mdata.empty:
            self.logger.error("{} has not data in {}".format(code, mdate))
            return None
        mdict = mdata.to_dict('records')[0]
        start = mdict['ibase']
        end = mdata.index.values[0]
        return data.loc[start:end]['profit'].max()

    def get_deleted_reason(self, code, mdate, timeToMarket, isleader, isAny = False):
        reasons = list()
        df = CStock(code).get_k_data(mdate)
        if df.empty: return '停牌'
        df['code'] = code
        df['timeToMarket'] = timeToMarket
        df['mv'] = df['totals'] * df['close'] / 100000000
        df['hlzh'] = df['ppercent'] - df['npercent']
        mdict = df.to_dict('records')[0]
        if mdict['timeToMarket'] > int((datetime.now() - timedelta(days = self.existed_days)).strftime('%Y%m%d')):
            if code not in set(ct.WHITE_DICT.keys()):
                msg = "{}上市时间少于5年".format(code)
                if isAny: return msg
                reasons.append(msg)
        if mdict['pday'] < self.pday:
            msg = "牛股时间小于{}天, 当前天数:{}".format(self.pday, mdict['pday'])
            if isAny: return msg
            reasons.append(msg)
        if mdict['mv'] <= self.min_market_val:
            msg = "市值小于{}亿, 当前市值:{}".format(self.min_market_val, mdict['mv'])
            if isAny: return msg
            reasons.append(msg)
        if mdict['hlzh'] <= self.min_hlzh:
            msg = "获利纵横小于{}, 实际值：{}".format(self.min_hlzh, mdict['hlzh'])
            if isAny: return msg
            reasons.append(msg)
        if mdict['ppercent'] <= self.min_ppercent:
            msg = "获利盘小于{}, 实际值：{}".format(self.min_ppercent, mdict['ppercent'])
            if isAny: return msg
            reasons.append(msg)
        if mdict['profit'] <= self.profit:
            msg = "基础浮动盈利小于{}".format(self.profit)
            if isAny: return msg
            reasons.append(msg)
        if code in set(EXCLUDE_LIST):
            msg = "{}在排除名单{}中".format(code, EXCLUDE_LIST)
            if isAny: return msg
            reasons.append(msg)
        if code in set(ct.BLACK_DICT.keys()):
            msg = "{}在黑名单中".format(code)
            if isAny: return msg
            reasons.append(msg)
        pledge_info = self.val_client.get_stock_pledge_info(code = code)
        if not pledge_info.empty and pledge_info.to_dict('records')[0]['pledge_rate'] >= self.pledge_rate:
            msg = "最新的质押率大于{}%".format(self.pledge_rate)
            if isAny: return msg
            reasons.append(msg)
        if df.apply(lambda row: self.get_val_in_range('rroe', row['code'], 'min'), axis = 1)[0] <= self.min_roe:
            msg = "净资产收益率最小值小于{}%".format(self.min_roe)
            if isAny: return msg
            reasons.append(msg)
        self.val_client.update_vertical_data(df, ['goodwill', 'ta'], transfer_date_string_to_int(mdate))
        df['gwr'] = 100 * df['goodwill'] / df['ta']
        if df.to_dict('records')[0]['gwr'] >= self.gwr:
            msg = "商誉占总资产的比例大于{}%, 实际值：{}".format(self.gwr, df.to_dict('records')[0]['gwr'])
            if isAny: return msg
            reasons.append(msg)
        max_profit = self.get_max_profit(code, mdate)
        if (not isleader) and (max_profit > self.max_profit):
            reasons.append("最大基础浮动盈利大于{}".format(self.max_profit))
        return '' if len(reasons) == 0 else '\n'.join(reasons)

    def get_new_data(self, mdate):
        df = self.rindex_client.get_data(mdate)
        if df is None: return pd.DataFrame()
        #黑名单
        black_set = set(ct.BLACK_DICT.keys())
        white_set = set(ct.WHITE_DICT.keys())
        if len(black_set.intersection(white_set)) > 0: raise Exception("black and white has intersection.")
        df['history'] = 0
        df['history'] = df.apply(lambda row: self.get_hist_val(black_set, white_set, row['code']), axis = 1)
        df = df[df['history'] > -1]
        df = df[~df.code.isin(EXCLUDE_LIST)]
        #添加上市时间和行业信息和三级行业信息
        base_df = self.stock_info_client.get()
        base_df = base_df[['code', 'name', 'timeToMarket', 'sw_industry']]
        df = pd.merge(df, base_df, how='inner', on=['code'])
        #确保上市时间小于5年
        start_time = int((datetime.now() - timedelta(days = self.existed_days)).strftime('%Y%m%d'))
        df = df[(df['timeToMarket'] < start_time) | df.code.isin(list(ct.WHITE_DICT.keys()))]
        #不买包含ST的股票
        df = df[~df.name.str.contains("ST")]
        #技术面选股
        df['hlzh'] = df['ppercent'] - df['npercent']
        df = df[(df.profit > self.profit) & (df.pday > self.pday) & (df.hlzh > self.hlzh)]
        #基本面选股
        #质押率
        pledge_info = self.val_client.get_stock_pledge_info()
        pledge_info = pledge_info[['code', 'pledge_rate']]
        df = pd.merge(df, pledge_info, how='left', on=['code'])
        df = df.fillna(value = {'pledge_rate': 0})
        df = df[df['pledge_rate'] < self.pledge_rate]
        #市值超过最小值, 小于最大值
        df['mv'] = df['totals'] * df['close'] / 100000000
        df = df[df['mv'] > self.min_market_val]
        #ROE中位数
        if df.empty: return df
        df['min_roe'] = df.apply(lambda row: self.get_val_in_range('rroe', row['code'], 'min'), axis = 1)
        df = df[df['min_roe'] > self.min_roe]
        #商誉上限
        if df.empty: return df
        self.val_client.update_vertical_data(df, ['goodwill', 'ta'], transfer_date_string_to_int(mdate))
        df['gwr'] = 100 * df['goodwill'] / df['ta']
        df = df[df['gwr'] < self.gwr]
        #最大基础浮动盈利 < 7，绩优股可以容忍大于7
        if df.empty: return df
        df['leader'] = False
        self.update_leading_status(df, base_df, mdate)
        df['max_profit'] = df.apply(lambda row: self.get_max_profit(row['code'], mdate), axis = 1)
        df = df[(df['max_profit'] < self.max_profit) | ((df['leader'] == True) & (df['max_profit'] >= self.max_profit))]
        if df.empty: return df
        df = df.dropna()
        df = df[['date', 'code', 'name', 'sw_industry', 'leader', 'timeToMarket']]
        df = df.reset_index(drop = True)
        return df

    def get_leading_codes(self, industry_data, mdate):
        if len(industry_data) < 5: return list()
        self.val_client.update_vertical_data(industry_data, ['revenue', 'rnp', 'rroe'], transfer_date_string_to_int(mdate))
        industry_data = industry_data.dropna(how='any')
        industry_data = industry_data.reset_index(drop = True)
        industry_data[["rroe", "rnp", "revenue"]] = industry_data[["rroe", "rnp", "revenue"]].apply(pd.to_numeric)
        top_count = max(3, int(len(industry_data) * 0.05))
        rnp_codes_set = set(industry_data.nlargest(top_count, 'rnp').code.tolist())
        rroe_codes_set = set(industry_data.nlargest(top_count, 'rroe').code.tolist())
        revenue_codes_set = set(industry_data.nlargest(top_count, 'revenue').code.tolist())
        a_list = list(rnp_codes_set & rroe_codes_set)
        b_list = list(rnp_codes_set & revenue_codes_set)
        c_list = list(revenue_codes_set & rroe_codes_set)
        return list(set(chain(a_list, b_list, c_list)))

    def update_leading_status(self, df, stock_info, mdate):
        for sw_industry, _ in df.groupby('sw_industry'):
            industry_data = stock_info.loc[stock_info.sw_industry == sw_industry]
            leading_codes = self.get_leading_codes(industry_data, mdate)
            if len(leading_codes) > 0:
                df.loc[(df.code.isin(leading_codes)) & (df.min_roe > 10), 'leader'] = True

    def update_days(self, code, exised_df):
        days_series = exised_df.loc[exised_df.code == code, 'days']
        return 1 if days_series.empty else days_series.values[0] + 1

    def compute_stock_pool(self, mdate):
        df = pd.DataFrame()
        pre_date = self.cal_client.pre_trading_day(mdate)
        pre_df = self.get_stock_pool(pre_date)
        now_df = self.get_new_data(mdate)
        if not now_df.empty and not pre_df.empty:
            #pre_df只处理今天不在股票池中的股票
            existed_df = pre_df.loc[pre_df.code.isin(now_df.code.tolist())]
            now_df['days'] = now_df.apply(lambda row: self.update_days(row['code'], existed_df), axis = 1)
            pre_df = pre_df.loc[~pre_df.code.isin(now_df.code.tolist())]
            if not pre_df.empty:
                pre_df['delete_reason'] = pre_df.apply(lambda row: self.get_deleted_reason(row['code'], mdate, row['timeToMarket'], row['leader'], True), axis = 1)
                pre_df['date'] = mdate
                #停牌个股的处理
                halted_df = pre_df[pre_df.delete_reason == '停牌']
                halted_df = halted_df.drop('delete_reason', axis = 1)
                #还应该在股票池中，但不在今天的新生成的股票池中的股票
                existed_df = pre_df[(pre_df.delete_reason == '') | (pre_df.delete_reason == '停牌')]
                existed_df = existed_df.drop(['delete_reason'], axis = 1)
                existed_df.loc[~existed_df.code.isin(halted_df.code.tolist()), 'days'] += 1
                df = pd.merge(now_df, existed_df, how='outer', on=['date', 'code', 'name', 'sw_industry', 'leader', 'timeToMarket', 'days'])
            else:
                df = now_df
        elif now_df.empty and not pre_df.empty:
            if not pre_df.empty:
                pre_df['delete_reason'] = pre_df.apply(lambda row: self.get_deleted_reason(row['code'], mdate, row['timeToMarket'], row['leader'], True), axis = 1)
                df = pre_df[(pre_df.delete_reason == '') | (pre_df.delete_reason == '停牌')]
                df.loc[(df.delete_reason == ''), 'days'] += 1
                df = df.drop('delete_reason', axis = 1)
                df['date'] = mdate
        else:
            df = now_df
            if not df.empty: df['days'] = 1
        df = df.reset_index(drop = True)
        return df

    def generate_feed(self, start_date, end_date):
        all_df = pd.DataFrame()
        feed = dataFramefeed.Feed()
        date_array = get_dates_array(start_date, end_date, asending = True)
        is_first = True
        code_list = list()
        for mdate in date_array:
            if self.cal_client.is_trading_day(mdate):
                df = self.get_stock_pool(mdate)
                if is_first:
                   code_list = df.code.tolist()
                   is_first = False
                if not df.empty: all_df = all_df.append(df)
        codes = list(set(all_df.code.tolist()))
        for code in codes:
            data = CStock(code).get_k_data()
            data = kdj(data)
            data = boll(data)
            data = data[(data.date >= start_date) & (data.date <= end_date)]
            data = data.sort_values(by=['date'], ascending = True)
            data = data.reset_index(drop = True)
            data = data.set_index('date')
            data.index = pd.to_datetime(data.index)
            data = data.dropna(how='any')
            feed.addBarsFromDataFrame(code, data)
        return feed, code_list

if __name__ == '__main__':
    #start_date = '2018-10-01'
    start_date = '2011-10-29'
    end_date   = '2019-10-31'
    redis_host = "127.0.0.1"
    dbinfo = ct.OUT_DB_INFO
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    sci_val_file_path = "/Volumes/data/quant/crawler/china_security_industry_valuation/stock" 
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    ftm = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo = dbinfo, redis_host = redis_host, should_create_mysqldb = True)
    ftm.generate_stock_pool(start_date, end_date)
    #ftm.get_new_data('2019-10-30')
