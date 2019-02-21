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
import matplotlib.pyplot as plt
from matplotlib.pylab import date2num
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from mpl_finance import candlestick_ohlc
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter
class OverSell():
    def __init__(self):
        self.ris = RIndexStock(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.base_color = '#e6daa6'
        self.fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        self.price_ax = plt.subplot2grid((12,12), (0,0), rowspan = 6, colspan = 12, facecolor = self.base_color, fig = self.fig)
        self.ratio_ax = plt.subplot2grid((12,12), (6,0), rowspan = 6, colspan = 12, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)

    def get_data(self, start_date, end_date, index_code):
        df = self.ris.get_k_data_in_range(start_date, end_date)
        iobj = CIndex(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        i_data = iobj.get_k_data_in_range(start_date, end_date)
        i_data['time'] = i_data.index.tolist()
        i_data = i_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return df, i_data

    def is_market_oversell(self, start_date, end_date, index_code):
        df, index_data = self.get_data(start_date, end_date, index_code)
        info = self.compute_stock_score(df)

    def get_oversell_stocks(self, df):
        data = df[(np.log(df['close']) - np.log(df['mprice'])) / np.log(0.9) > 1]
        return data.code.tolist()

    def compute_stock_score(self, data):
        code_list = list()
        date_list = list()
        rate_list = list()
        oversell_ratio = 0
        for cdate, df in data.groupby(data.date):
            total_num = len(df)
            oversell_code_list = self.get_oversell_stocks(df)
            oversold_num = len(oversell_code_list)
            oversell_ratio = 100 * oversold_num / total_num
            date_list.append(cdate)
            rate_list.append(oversell_ratio)
            code_list.append(oversell_code_list)
        info = {'date':date_list, 'rate':rate_list, 'code':code_list}
        df = pd.DataFrame(info)
        return df

    def plot(self, start_date, end_date, index_code):
        df, index_data = self.get_data(start_date, end_date, index_code)
        date_tickers = index_data.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]
        info = self.compute_stock_score(df)
        candlestick_ohlc(self.price_ax, index_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.ratio_ax.plot(info['date'], info['rate'], 'r',  label = "超跌系数", linewidth = 1)
        self.price_ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
        self.price_ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        plt.show()

if __name__ == '__main__':
    start_date = '2002-04-01' 
    end_date = '2019-02-20'
    code = '000001'
    cbr = OverSell()
    cbr.plot(start_date, end_date, code)
