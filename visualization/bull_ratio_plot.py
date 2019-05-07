#coding = utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import numpy as np
import pandas as pd
import datetime as dt
from cindex import CIndex
from rstock import RIndexStock
from datamanager.bull_stock_ratio import BullStockRatio
import matplotlib.pyplot as plt
from matplotlib.pylab import date2num
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from mpl_finance import candlestick_ohlc
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter
class CBullRation():
    def __init__(self, dinfo, rhost):
        self.dbinfo = dinfo
        self.redis_host = rhost
        self.base_color = '#e6daa6'
        self.ris = RIndexStock(dbinfo = self.dbinfo, redis_host = self.redis_host)

    def get_index_data(self, start_date, end_date, index_code):
        iobj = CIndex(index_code, dbinfo = self.dbinfo, redis_host = self.redis_host)
        i_data = iobj.get_k_data_in_range(start_date, end_date)
        i_data = i_data.sort_values(by=['date'], ascending=True)
        i_data = i_data.reset_index(drop = True)
        i_data['time'] = i_data.index.tolist()
        i_data = i_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return i_data

    def get_bull_ratios(self, index_code, start_date, end_date):
        obj = BullStockRatio(index_code, dbinfo = self.dbinfo, redis_host = self.redis_host)
        df = obj.get_k_data_between(start_date, end_date)
        df = df.sort_values(by=['date'], ascending=True)
        df = df.reset_index(drop = True)
        return df

    def plot(self, start_date, end_date, index_code):
        fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        price_ax = plt.subplot2grid((12,12), (0,0), rowspan = 6, colspan = 12, facecolor = self.base_color, fig = fig)
        ratio_ax = plt.subplot2grid((12,12), (6,0), rowspan = 6, colspan = 12, facecolor = self.base_color, sharex = price_ax, fig = fig)
        info = self.get_bull_ratios(index_code, start_date, end_date)
        index_data = self.get_index_data(start_date, end_date, index_code)
        date_tickers = index_data.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]
        candlestick_ohlc(price_ax, index_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        ratio_ax.plot(info['date'], info['ratio'], 'r',  label = "股票牛股比例", linewidth = 1)
        price_ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
        price_ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        plt.show()

if __name__ == '__main__':
    start_date = '2019-01-01' 
    end_date = '2019-05-07'
    code = '000001'
    cbr = CBullRation(ct.OUT_DB_INFO, '127.0.0.1')
    cbr.plot(start_date, end_date, code)
