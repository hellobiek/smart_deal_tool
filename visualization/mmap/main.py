# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import pandas as pd
from rstock import RIndexStock
from cstock_info import CStockInfo
from industry_info import IndustryInfo
from common import is_df_has_unexpected_data
if __name__ == '__main__':
    try:
        mdate = '2019-08-02'
        cobj = CStockInfo()
        robj = RIndexStock()
        iobj = IndustryInfo()
        black_list = list(ct.BLACK_DICT.keys())

        bdf = cobj.get()
        stock_info = robj.get_data(mdate)
        idf = iobj.get_csi_industry_data(mdate)
        df = pd.merge(bdf, idf, how='left', on=['code'])
        df = pd.merge(stock_info, df, how='inner', on=['code'])
        df = df[~df.code.isin(black_list)]
        df = df[(df.profit > 1) & (df.profit < 3) & (df.pday > 30) & (df.timeToMarket < 20150101)]
        df = df.reset_index(drop = True)
        #df = df[['code', 'name', 'industry', 'profit', 'pday', 'pind_name', 'sind_name', 'tind_name', 'find_name']]
        df = df[['code', 'name', 'profit', 'pday', 'find_name']]
        for name, contains in df.groupby('find_name'):
            if len(contains) > 2:
                print("--------------------")
                print(name)
                print(contains)
                print("====================")
        print("total num", len(df))
    except Exception as e:
        print(e)
        traceback.print_exc()
