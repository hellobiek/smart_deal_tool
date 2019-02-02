#coding=utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import numpy as np
import pandas as pd
import datetime as dt
from numpy import array
from cindex import CIndex
from rstock import RIndexStock
from datamanager.sexchange import StockExchange
import matplotlib.pyplot as plt
from matplotlib.pylab import date2num
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from mpl_finance import candlestick_ohlc
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter
class CMarketValue():
    def __init__(self):
        self.base_color = '#e6daa6'
        self.ris = RIndexStock(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.sh_market_client = StockExchange(ct.SH_MARKET_SYMBOL, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.sz_market_client = StockExchange(ct.SZ_MARKET_SYMBOL, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        self.price_ax = plt.subplot2grid((12,12), (0,0), rowspan = 6, colspan = 12, facecolor = self.base_color, fig = self.fig)
        self.ratio_ax = plt.subplot2grid((12,12), (6,0), rowspan = 6, colspan = 12, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)

    def get_data(self, start_date, end_date):
        sh_df = self.get_market_data(start_date, end_date, ct.SH_MARKET_SYMBOL)
        sz_df = self.get_market_data(start_date, end_date, ct.SZ_MARKET_SYMBOL)
        date_list = sh_df.date.tolist()
        sh_neg_list = array(sh_df.negotiable_value.tolist())
        sz_neg_list = array(sz_df.negotiable_value.tolist())
        neg_list = (sh_neg_list + sz_neg_list) / 2
        info = {'date':date_list, 'rate':neg_list.tolist()}
        return info

    def get_market_data(self, start_date, end_date, market):
        if market == ct.SH_MARKET_SYMBOL:
            df = self.sh_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '上海市场']
        else:
            df = self.sz_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '深圳市场']
        df = df.round(2)
        df = df.drop_duplicates()
        df = df.reset_index(drop = True)
        df = df.sort_values(by = 'date', ascending= True)
        df.negotiable_value = (df.negotiable_value / 2).astype(int)
        return df
   
    def get_index_data(self, start_date, end_date, index_code):
        iobj = CIndex(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        i_data = iobj.get_k_data_in_range(start_date, end_date)
        i_data['time'] = i_data.index.tolist()
        i_data = i_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return i_data

    def plot(self, start_date, end_date, index_code):
        index_data = self.get_index_data(start_date, end_date, index_code)
        info = self.get_data(start_date, end_date)
        date_tickers = index_data.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]
        candlestick_ohlc(self.price_ax, index_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.ratio_ax.plot(info['date'], info['rate'], 'r',  label = "超跌系数", linewidth = 1)
        self.price_ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
        self.price_ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        plt.show()

if __name__ == '__main__':
    start_date = '2017-12-29' 
    end_date = '2019-01-29'
    index_code = '000001'
    cmv = CMarketValue()
    cmv.plot(start_date, end_date, index_code)
