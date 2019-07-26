# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import pandas as pd
from cstock_info import CStockInfo
from cpython.cval import CValuation
class MValuation(object):
    def __init__(self, valution_path = ct.VALUATION_PATH):
        self.val_client = CValuation()
        self.report_data_path = valution_path
        self.stock_info_client = CStockInfo()

    def get_horizontal_data(self, code, dtype_list):
        pass

    def get_vertical_data(self, df, dtype_list, mdate):
        def cfun(code, time2Market):
            cur_item = self.val_client.get_actual_report_item(mdate, time2Market, code)
            return 0 if len(cur_item) == 0 else item[dtype]
        import pdb
        pdb.set_trace()
        for dtype in dtype_list:
            df[dtype] = df.apply(lambda df: func(df['code'], df['timeToMarket']), axis = 1)

if __name__ == '__main__':
    mvaluation = MValuation()
    import pdb
    pdb.set_trace()
    base_df = mvaluation.stock_info_client.get_basics()
    dtype_list = ['']
