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
from scipy.stats import percentileofscore
if __name__ == '__main__':
    mdate = 20190110
    dtype_list = ['dar']
    val_client = CValuation()
    stock_client = CStockInfo()
    df = stock_client.get()
    starttime = datetime.datetime.now()
    vdf = val_client.get_vertical_data(df, dtype_list, mdate)
    values = vdf['dar'].tolist()
    hist, edges = np.histogram(values, density=False, bins=100)
    import pdb
    pdb.set_trace()
    endtime = datetime.datetime.now()
    print((endtime - starttime).seconds)
