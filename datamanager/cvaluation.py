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
from rstock import RIndexStock
from ccalendar import CCalendar
from base.clog import getLogger
from cstock_info import CStockInfo
from cpython.cval import CValuation
from base.cdate import get_day_nday_ago, get_dates_array, transfer_int_to_date_string
class MValuation(object):
    def __init__(self):
        self.logger = getLogger(__name__)
        self.cval_client = CValuation()
        self.stock_info_client = CStockInfo()
        self.TYPE_LIST = ['bps', 'tcs', 'roa', 'dar', 'npm', 'gpr', 'revenue', 'cfpsfo', 'ngr','igr', 'crr', 'ncf', 'ta', 'fa', 'ca', 'micc', 'iar', 'cip', 'ar',
                          'br', 'stb', 'inventory', 'mf', 'goodwill', 'pp', 'qfii_holders', 'qfii_holding', 'social_security_holding', 'social_security_holders']

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

if __name__ == '__main__':
    mval_client = MValuation()
    mval_client.update_index(end_date = '2019-08-13')
