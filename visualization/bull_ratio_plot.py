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
from rprofit import RProfit
from rstock import RIndexStock
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
        i_data['time'] = i_data.index.tolist()
        i_data = i_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return i_data

    def get_data(self, start_date, end_date):
        uprice_df = self.ris.get_k_data_in_range(start_date, end_date)
        return uprice_df

    def get_components(self, code, cdate):
        iobj = CIndex(code)
        df = iobj.get_components_data(cdate)
        if code == '000001': df = df[df.code.str.startswith('6')]
        return df.code.tolist()

    def get_profit_stocks(self, df):
        data = df[df.profit >= 0]
        return data.code.tolist()

    def get_bull_ratios(self, data, ccode_list):
        code_list = list()
        date_list = list()
        rate_list = list()
        for cdate, df in data.groupby(data.date):
            df = df[df.code.isin(ccode_list)]
            profit_code_list = self.get_profit_stocks(df)
            bull_stock_num = len(profit_code_list)
            bull_ration = 100 * bull_stock_num / len(df)
            date_list.append(cdate)
            rate_list.append(bull_ration)
            code_list.append(profit_code_list)
        info = {'date':date_list, 'rate':rate_list, 'code':code_list}
        df = pd.DataFrame(info)
        return pd.DataFrame(info)

    def plot(self, start_date, end_date, index_code):
        code_list = self.get_components(index_code, end_date)
        uprice_df = self.get_data(start_date, end_date)
        index_data = self.get_index_data(start_date, end_date, index_code)
        date_tickers = index_data.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]
        info = self.get_bull_ratios(uprice_df, code_list)
        candlestick_ohlc(self.price_ax, index_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.ratio_ax.plot(info['date'], info['rate'], 'r',  label = "uprice 成本均线", linewidth = 1)
        self.price_ax.xaxis.set_major_locator(mticker.MultipleLocator(20))
        self.price_ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        plt.show()

if __name__ == '__main__':
    start_date = '2017-06-09' 
    end_date = '2019-02-01'
    code = '399006'
    cbr = CBullRation()
    cbr.plot(start_date, end_date, code)
