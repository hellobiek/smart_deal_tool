#coding=utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import const as ct
import pandas as pd
from common import get_day_nday_after, get_dates_array
from cindex import CIndex
import matplotlib.pyplot as plt
from matplotlib import ticker as mticker
from mpl_finance import candlestick2_ochl
def plot_index(cdate, i_data):
    base_color = '#e6daa6'
    name = "shanghai_%s_image" % cdate
    i_data = i_data[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    fig, index_ax = plt.subplots(figsize = (16, 10))
    fig.subplots_adjust(bottom = 0.2)
    candlestick2_ochl(index_ax, i_data['open'], i_data['close'], i_data['high'], i_data['low'], width = 1.0, colorup = 'r', colordown = 'g')
    index_ax.set_ylabel(name)
    index_ax.set_xticks(range(0, len(i_data['date']), 10))
    plt.plot()
    fig.autofmt_xdate()
    plt.show()

index_obj = CIndex('000001', dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
df = index_obj.get_k_data()
cdates = df[(df.pchange > 4) & (df.date > '1999-01-01')].date.tolist()
for mdate in cdates:
    start_date = mdate
    end_date = get_day_nday_after(start_date, num = 40, dformat = "%Y-%m-%d")
    index_data = index_obj.get_k_data_in_range(start_date, end_date)
    plot_index(start_date, index_data)

