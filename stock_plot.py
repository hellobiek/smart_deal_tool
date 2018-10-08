#coding=utf-8
import os
import sys
import numpy as np
import pandas as pd
from cindex import CIndex
from cstock import CStock
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter

class CPlot():
    def __init__(self, code, index_code = '000001'):
        self.code = code
        self.index_code = index_code
        self.base_color = '#e6daa6'
        self.k_data, self.d_data, self.i_data, self.init_date = self.read_data()
        self.volumeMin = 0
        self.volumeMax = 0
        self.priceMin = 0
        self.priceMax = 0
        self.dateMin = 0
        self.dateMax = 0
        self.fig = plt.figure(facecolor = self.base_color, figsize = (24, 24))
        self.price_ax  = plt.subplot2grid((12,12), (0,0), rowspan = 7, colspan = 8, facecolor = self.base_color, fig = self.fig)
        self.index_ax  = plt.subplot2grid((12,12), (7,0), rowspan = 4, colspan = 8, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)
        self.volume_ax = plt.subplot2grid((12,12), (11,0), rowspan = 1, colspan = 8, facecolor = self.base_color, sharex = self.price_ax, fig = self.fig)
        self.dist_ax   = plt.subplot2grid((12,12), (0,8), rowspan = 7, colspan = 4, facecolor = self.base_color, sharey = self.price_ax, fig = self.fig)

        self.press = None
        self.release = None
        self.keypress = self.fig.canvas.mpl_connect('key_press_event', self.on_key_press)
        self.cidpress = self.fig.canvas.mpl_connect('button_press_event', self.on_press)
        self.cidrelease = self.fig.canvas.mpl_connect('button_release_event', self.on_release)

        self.multi = MultiCursor(self.fig.canvas, (self.price_ax, self.volume_ax, self.index_ax), color='b', lw=1, horizOn = True, vertOn = True)

    def __del__(self):
        self.fig.canvas.mpl_disconnect(self.keypress)
        self.fig.canvas.mpl_disconnect(self.cidpress)
        self.fig.canvas.mpl_disconnect(self.cidrelease)
        plt.close(self.fig)

    def read_data(self):
        if not os.path.exists('i_data.json'):
            obj = CStock(self.code)
            k_data = obj.get_k_data()
            with open('k_data.json', 'w') as f:
                f.write(k_data.to_json(orient='records', lines=True))

            d_data = obj.get_chip_distribution()
            with open('d_data.json', 'w') as f:
                f.write(d_data.to_json(orient='records', lines=True))

            iobj = CIndex(self.index_code)
            i_data = iobj.get_k_data()
            cdates = k_data.cdate.tolist()
            i_data = i_data.loc[i_data.cdate.isin(cdates)]
            with open('i_data.json', 'w') as f:
                f.write(i_data.to_json(orient='records', lines=True))
            sys.exit(0)
        else:
            with open('k_data.json', 'r') as f:
                k_data = pd.read_json(f.read(), orient = 'records', lines = True)
            with open('d_data.json', 'r') as f:
                d_data = pd.read_json(f.read(), orient = 'records', lines = True)
            with open('i_data.json', 'r') as f:
                i_data = pd.read_json(f.read(), orient = 'records', lines = True)

            k_data = k_data[['cdate', 'open', 'high', 'low', 'close', 'volume', 'amount', 'outstanding', 'totals', 'adj', 'aprice', 'uprice', '60price']]
            k_data = k_data.rename(columns = {"cdate": "time"})

            init_date = k_data.time.tail(1).values[0]
            k_data.time = pd.to_datetime(k_data.time, format='%Y-%m-%d')
            k_data.time = mdates.date2num(k_data.time)
            k_data.time = k_data.time.astype(int)

            i_data = i_data[['cdate', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            i_data = i_data.rename(columns = {"cdate": "time"})
            i_data.time = pd.to_datetime(i_data.time, format='%Y-%m-%d')
            i_data.time = mdates.date2num(i_data.time)
            i_data.time = i_data.time.astype(int)

            return k_data, d_data, i_data, init_date

    def on_key_press(self, event):
        if event.key in 'Rr':
            self.plot()

    def on_press(self, event):
        self.press = event.xdata 

    def on_release(self, event):
        self.release = event.xdata
        if self.press is not None and self.release is not None:
            start_date = self.press
            end_date = self.release
            if start_date == end_date:
                tdate = mdates.num2date(start_date).strftime("%Y-%m-%d")
                self.plot_distribution(self.d_data, tdate)
            else:
                k_data = self.k_data.loc[(self.k_data.time >= start_date) & (self.k_data.time <= end_date)]
                i_data = self.i_data.loc[(self.i_data.time >= start_date) & (self.i_data.time <= end_date)]
                self.plot_stock(k_data)
                self.plot_volume(k_data)
                self.plot_index(i_data)
                self.plot_distribution(self.d_data, start_date)
                self.fig.suptitle(self.code, color='k')
                self.fig.autofmt_xdate()

    def plot_volume(self, k_data):
        self.volumeMax = k_data.volume.values.max()
        self.volume_ax.set_ylim(self.volumeMin, self.volumeMax)
        self.volume_ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        self.volume_ax.xaxis.set_major_locator(mticker.MaxNLocator(20))
        self.volume_ax.yaxis.label.set_color("k")
        self.volume_ax.set_ylabel("volumes")
        self.volume_ax.fill_between(k_data.time, self.volumeMin, k_data.volume, facecolor = 'b', alpha = 1)
        self.volume_ax.grid(True, color = 'k', linestyle = '--')

    def plot_index(self, i_data):
        from mpl_finance import candlestick_ohlc
        candlestick_ohlc(self.index_ax, i_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.index_ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        self.index_ax.xaxis.set_major_locator(mticker.MaxNLocator(20))
        self.index_ax.set_ylabel("Shanghai")
        self.index_ax.yaxis.label.set_color("k")
        self.index_ax.grid(True, color = 'k', linestyle = '--')

    def plot_stock(self, k_data):
        from mpl_finance import candlestick_ohlc
        self.priceMax = k_data.high.values.max()
        self.dateMin  = k_data.time.values.min()
        self.dateMax  = k_data.time.values.max()
        candlestick_ohlc(self.price_ax, k_data.values, width = 1.0, colorup = 'r', colordown = 'g')
        self.price_ax.plot(k_data.time, k_data['60price'], 'm', label = "60日成本均线", linewidth = 1)
        self.price_ax.plot(k_data.time, k_data['uprice'], 'b',  label = "无穷成本均线", linewidth = 1)
        self.price_ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        self.price_ax.xaxis.set_major_locator(mticker.MaxNLocator(20))
        self.price_ax.yaxis.label.set_color('k')
        self.price_ax.set_ylabel("prices")
        self.price_ax.set_xlim(self.dateMin, self.dateMax)
        self.price_ax.set_ylim(self.priceMin, self.priceMax)
        self.price_ax.grid(True, color = 'k', linestyle = '--')
 
    def plot_distribution(self, d_data, tdate):
        self.dist_ax.clear()
        self.dist_ax.xaxis.label.set_color("k")
        self.dist_ax.grid(True, color = 'k', linestyle = '--')
        tmp_df = d_data.loc[d_data.date == tdate]
        if tmp_df.empty:
            self.dist_ax.set_xlabel("%s no data" % tdate)
        else:
            volumeMax = tmp_df.volume.values.max()
            self.dist_ax.set_xlabel(tdate)
            self.dist_ax.set_xlim(self.volumeMin, volumeMax)
            self.dist_ax.set_ylim(self.priceMin, self.priceMax)
            self.dist_ax.barh(tmp_df.price, tmp_df.volume, height = 0.3, facecolor = 'blue', alpha = 1)

    def plot(self):
        self.plot_stock(self.k_data)
        self.plot_volume(self.k_data)
        self.plot_index(self.i_data)
        self.plot_distribution(self.d_data, self.init_date)
        self.fig.suptitle(self.code, color='k')
        self.fig.autofmt_xdate()
        plt.show()

if __name__ == '__main__':
    cp = CPlot('601318')
    cp.plot()
