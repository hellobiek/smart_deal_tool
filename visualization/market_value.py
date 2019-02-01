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
class CMarketValue():
    def __init__(self):
        self.base_color = '#e6daa6'
        self.ris = RIndexStock(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        self.sh_market_client = StockExchange(ct.SH_MARKET_SYMBOL)
        self.sz_market_client = StockExchange(ct.SZ_MARKET_SYMBOL)
        self.fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        self.price_ax = plt.subplot2grid((12,12), (0,0), rowspan = 6, colspan = 12, facecolor = self.base_color, fig = self.fig)
        self.ratio_ax = plt.subplot2grid((12,12), (6,0), rowspan = 6, colspan = 12, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)

    def get_market_data(self, market, start_date, end_date):
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
