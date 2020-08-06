# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import datetime
import numpy as np
from pandas import Series
from cstock_info import CStockInfo
from cpython.cval import CValuation
from base.cdate import quarter, report_date_with, int_to_datetime, prev_report_date_with, get_pre_date, get_next_date, pre_report_date_with
if __name__ == '__main__':
    mdate = 20200807
    dtype_list = ['social_security_holders']
    val_client = CValuation()
    stock_client = CStockInfo()
    df = stock_client.get()
    xx = df.loc[df.code == '600217']
    xx = xx.reset_index(drop = True)
    import pdb
    pdb.set_trace()
    vdf = val_client.update_vertical_data(xx, dtype_list, mdate)
    import pdb
    pdb.set_trace()
