# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import xlrd
import time
import calendar
import traceback
import const as ct
import numpy as np
import pandas as pd
from pathlib import Path
from cstock import CStock
from gevent.pool import Pool
from functools import partial
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from cstock_info import CStockInfo
from cpython.cval import CValuation
from base.cdate import quarter, report_date_with, int_to_datetime, prev_report_date_with, get_pre_date, get_next_date, get_day_nday_ago, get_dates_array
TYPE_LIST = ['bps', 'tcs', 'roa', 'dar', 'npm', 'gpr', 'revenue', 'cfpsfo', 'ngr', 'igr', 'crr', 
            'ncf', 'ta', 'fa', 'ca', 'micc', 'iar', 'cip', 'ar', 'br', 'stb', 'inventory', 'mf', 
            'goodwill', 'pp', 'qfii_holders', 'qfii_holding', 'social_security_holding', 'social_security_holders']
class MValuation(object):
    def __init__(self):
        self.logger = getLogger(__name__)
        self.cval_client = CValuation()
        self.stock_info_client = CStockInfo()

    def collect_financial_data(self, spath = "/data/crawler/china_security_industry_valuation/stock", tpath = '/data/valuation/cstocks'):
        def myfunc(code, mdate):
            tmp_df = df.loc[(df.code == code) & (df.date == mdate)]
            tmp_df = tmp_df.reset_index(drop = True)
            CStock(code).set_val_data(tmp_df, fpath = "/data/valuation/cstocks")
        spath_obj = Path(spath)
        csvs = spath_obj.glob('*.csv')
        xfiles = [xfile.name for xfile in csvs]
        xfiles.sort()
        use_cols = ['code', 'date', 'pe', 'pb', 'ttm', 'dividend']
        dtype_dict = {'code':str, 'date': str, 'pe': float, 'pb': float, 'ttm': float, 'dividend': float}
        for fname in xfiles:
            df = pd.read_csv(spath_obj / fname, header = 0, encoding = "utf8", usecols = use_cols, dtype = dtype_dict)
            df = df[use_cols]
            vfunc = np.vectorize(myfunc)
            vfunc(df['code'].values, df['date'].values)

    def set_financial_data(self, code = '688122', mdate = '2019-07-30'):
        try:
            df = self.stock_info_client.get()
            code_list = df['code'].tolist()
            time2market_list = df['timeToMarket'].tolist()
            code2timedict = dict(zip(code_list, time2market_list))
            self.cval_client.set_stock_valuation(code2timedict, mdate, code)
        except Exception as e:
            self.logger.error(e)
            traceback.print_exc()

    def set_r_financial_data(self, mdate, code_list):
        def cget(mdate, code):
            return code, CStock(code).get_val_data(mdate)
        try:
            obj_pool = Pool(5000)
            all_df = pd.DataFrame()
            cfunc = partial(cget, mdate)
            for code_data in obj_pool.imap_unordered(cfunc, code_list):
                if code_data[1] is not None and not code_data[1].empty:
                    tem_df = code_data[1]
                    tem_df['code'] = code_data[0]
                    all_df = all_df.append(tem_df)
            obj_pool.join(timeout = 5)
            obj_pool.kill()
            all_df = all_df.reset_index(drop = True)
            file_name = self.get_r_financial_name(mdate)
            file_path = Path(self.rvaluation_dir) / file_name
            all_df.to_csv(file_path, index=False, header=True, mode='w', encoding='utf8')
            return True
        except Exception as e:
            self.logger.error(e)
            traceback.print_exc()
            return False

    def update_index(self, end_date = datetime.now().strftime('%Y-%m-%d'), num = 3361):
        succeed = True
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date, asending = True)
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate):
                for code in ct.INDEX_DICT:
                    if not self.cval_client.set_index_valuation(code, mdate):
                        self.logger.error("{} set {} data for rvaluation failed".format(code, mdate))
                        succeed = False
        return succeed

    def update(self, end_date = datetime.now().strftime('%Y-%m-%d'), num = 7):
        succeed = True
        base_df = self.stock_info_client.get_basics()
        code_list = base_df.code.tolist()
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate):
                if not self.set_r_financial_data(mdate, code_list):
                    self.logger.error("set %s data for rvaluation failed" % mdate)
                    succeed = False
        return succeed

def get_hist_val(black_set, white_set, code):
    if code in white_set:
        return 1
    elif code in black_set:
        return -1
    else:
        return 0

#if __name__ == '__main__':
#    mval_client = MValuation()
#    mval_client.update_index(end_date = '2019-08-13')

if __name__ == '__main__':
    try:
        mdate = 20190802 
        mval_client = MValuation()
        #黑名单
        black_set = set(ct.BLACK_DICT.keys())
        white_set = set(ct.WHITE_DICT.keys())
        if len(black_set.intersection(white_set)) > 0: raise Exception("black and white has intersection.")
        df = mval_client.stock_info_client.get()
        df['history'] = df.apply(lambda row: get_hist_val(black_set, white_set, row['code']), axis = 1)
        #质押信息
        pledge_info = mval_client.cval_client.get_stock_pledge_info()
        pledge_info = pledge_info[['code', 'pledge_rate']]
        df = pd.merge(df, pledge_info, how='inner', on=['code'])
        df = df.reset_index(drop = True)
        #净资产收益率
        mval_client.cval_client.update_vertical_data(df, TYPE_LIST, mdate)

        #应收款率 = (应收帐款 + 应收票据) / 总资产
        df['arr'] = 100 * (df['ar'] + df['br']) / df['ta']
        #商誉占比
        df['gwr'] = 100 * df['goodwill'] / df['ta']
        #货币资金达到资产总额
        df['mfr'] = 100 * (df['mf'] - df['stb']) / df['ta']
        #短期借款占比
        df['stbr'] = 100 * df['stb'] / df['ta']
        #在建工程占比
        df['cipr'] = 100 * df['cip'] / df['ta']
        #应付职工薪酬占比
        df['ppr'] = 100 * df['pp'] / df['ta']
        #开始选股
        df = df.dropna(subset = TYPE_LIST)
        df = df[(df['timeToMarket'] < 20151231) | df.code.isin(list(ct.WHITE_DICT.keys()))]
        df = df[df['pledge_rate'] < 30]
        df = df[df['roa'] > 2]
        df = df[df['dar'] < 45]
        df = df[df['history'] > -1]
        df = df[df['gwr'] < 30]
        df = df[df['iar'] < 30]
        df = df[df['ppr'] < 3]
        df = df[df['arr'] < 30]
        df = df[df['mfr'] > 10]
        df = df.reset_index(drop = True)
        df = df[['code', 'name', 'industry', 'history', 'roa', 'pledge_rate', 'arr', 'dar', 'iar', 'gwr', 'mfr', 'ppr', 'cipr', 'stbr', 'qfii_holders', 'qfii_holding', 'social_security_holding']]
        for name, contains in df.groupby('industry'):
            print("--------------------")
            print(name)
            print(contains)
            print("====================")
        print("total num", len(df))
    except Exception as e:
        print(e)
        traceback.print_exc()
