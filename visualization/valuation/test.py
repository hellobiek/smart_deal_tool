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
def get_vertical_data(cobj, df, dtype_list, mdate, industry = '所有'):
    def cfunc(code, time2Market):
        item = cobj.get_actual_report_item(mdate, code, time2Market)
        if 1 == len(dtype_list):
            return item[dtype_list[0]] if item else None
        else:
            return tuple([item[dtype] for dtype in dtype_list]) if item else tuple([0.0 for dtype in dtype_list])
    vfunc = np.vectorize(cfunc)
    for dtype, dval in zip(dtype_list, vfunc(df['code'].values, df['timeToMarket'].values)):
        df[dtype] = dval
    df = df.dropna(subset = dtype_list)
    df = df[(df[dtype_list] > 0).all(axis=1)]
    df = df.reset_index(drop = True)
    return df


if __name__ == '__main__':
    mdate = 20190110
    dtype_list = ['roe']
    val_client = CValuation()
    stock_client = CStockInfo()
    df = stock_client.get()
    starttime = datetime.datetime.now()
    #vdf = val_client.get_vertical_data(df, dtype_list, mdate)
    vdf = get_vertical_data(val_client, df, dtype_list, mdate)
    import pdb
    pdb.set_trace()
    endtime = datetime.datetime.now()
    print((endtime - starttime).seconds)
