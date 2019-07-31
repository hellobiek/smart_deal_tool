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
class MValuation(object):
    def __init__(self):
        self.logger = getLogger(__name__)
        self.cval_client = CValuation()

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

    def set_financial_data(self, mdate = ''):
        '''
        计算PE、PB、ROE、股息率、流通股本、总股本、流通市值、总市值
        1.基本每股收益、4.每股净资产、96.归属于母公司所有者的净利润、238.总股本、239.已上市流通A股
            总市值=当前股价×总股本
            PE=股价/每股收益
            PB=股价/每股净资产
            ROE=利润/每股净资产=PB/PE : 财报中已有静态的净资产收益率数据, 这里通过TTM计算一个大概的ROE作为参考
        '''
        try:
            base_df = self.stock_info_client.get_basics()
            #fpath = "/tmp/succeed_list"
            #with open(fpath) as f: succeed_list = f.read().strip().split()
            for row in base_df.itertuples():
                code = row.code
                #if code not in succeed_list:
                if code == '000693':
                    timeToMarket = row.timeToMarket
                    self.set_stock_valuation(mdate, code, timeToMarket)
                    #if self.set_stock_valuation(mdate, code, timeToMarket):
                    #    succeed_list.append(code)
                    #    with open(fpath, 'a+') as f: f.write(code + '\n')
                    #    pass
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

    def update_val(self, end_date = datetime.now().strftime('%Y-%m-%d'), num = 7000):
        succeed = True
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate):
                for code in ct.INDEX_DICT:
                    if not self.set_index_valuation(code, mdate):
                        self.logger.error("%s set %s data for rvaluation failed" % (code, mdate))
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
    try:
        mval_client = MValuation()
        stock_client = CStockInfo()
        black_list = list(ct.BLACK_DICT.keys())
        white_list = list(ct.WHITE_DICT.keys())
        df = stock_client.get()
        df = df.loc[~df.code.isin(black_list)]
        #质押信息
        pledge_info = mval_client.cval_client.get_stock_pledge_info()
        pledge_info = pledge_info.loc[pledge_info.code.isin(df.code.tolist())]
        pledge_info = pledge_info.sort_values(by=['pledge_rate'], ascending = False)
        pledge_info = pledge_info.reset_index(drop = True)
        pledge_info = pledge_info.loc[pledge_info['pledge_rate'] <= 20]
        pledge_info_code_list = pledge_info.code.tolist()
        df = df.loc[df.code.isin(pledge_info_code_list)]
        import pdb
        pdb.set_trace()
    except Exception as e:
        print(e)
