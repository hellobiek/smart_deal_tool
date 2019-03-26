#coding=utf-8
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
    def __init__(self):
        self.ris = RIndexStock(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.base_color = '#e6daa6'
        self.fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        self.price_ax = plt.subplot2grid((12,12), (0,0), rowspan = 6, colspan = 12, facecolor = self.base_color, fig = self.fig)
        self.ratio_ax = plt.subplot2grid((12,12), (6,0), rowspan = 6, colspan = 12, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)

    def get_index_data(self, start_date, end_date, index_code):
        iobj = CIndex(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        i_data = iobj.get_k_data_in_range(start_date, end_date)
        i_data = i_data.sort_values(by=['date'], ascending=True)
        i_data = i_data.reset_index(drop = True)
        i_data['time'] = i_data.index.tolist()
        i_data = i_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return i_data

    def get_components(self, code, cdate):
        iobj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        df = iobj.get_components_data(cdate)
        if code == '000001': df = df[df.code.str.startswith('6')]
        return df.code.tolist()

    def get_profit_stocks(self, df):
        data = df[df.profit >= 0]
        return data.code.tolist()

    def get_bull_ratios(self, index_code, start_date, end_date):
        obj = BullStockRatio(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        df = obj.get_k_data_between(start_date, end_date)
        df = df.sort_values(by=['date'], ascending=True)
        df = df.reset_index(drop = True)
        return df

    def plot(self, start_date, end_date, index_code):
        info = self.get_bull_ratios(index_code, start_date, end_date)
        index_data = self.get_index_data(start_date, end_date, index_code)
        date_tickers = index_data.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]
        candlestick_ohlc(self.price_ax, index_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.ratio_ax.plot(info['date'], info['ratio'], 'r',  label = "股票牛股比例", linewidth = 1)
        self.price_ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
        self.price_ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        plt.show()

if __name__ == '__main__':
    start_date = '2017-06-25' 
    end_date = '2019-03-11'
    code = '880883'
    cbr = CBullRation()
    cbr.plot(start_date, end_date, code)
