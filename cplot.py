#coding=utf-8
import os
import sys
import const as ct
import numpy as np
import pandas as pd
from cindex import CIndex
from cstock import CStock
from pandas import DataFrame, Series
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY, YEARLY
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from matplotlib.dates import MonthLocator, MONTHLY

def read_data(code, index_code = '000001'):
    if not os.path.exists('i_data.json'):
        obj = CStock(code)
        k_data = obj.get_k_data()
        with open('k_data.json', 'w') as f:
            f.write(k_data.to_json(orient='records', lines=True))
        d_data = obj.get_chip_distribution()
        if not os.path.exists('d_data.json'):
            with open('d_data.json', 'w') as f:
                f.write(d_data.to_json(orient='records', lines=True))
        cdates = k_data.cdate.tolist()
        iobj = CIndex(ct.DB_INFO, index_code)
        i_data = iobj.get_k_data()
        i_data = i_data.loc[i_data.cdate.isin(cdates)]
        if not os.path.exists('i_data.json'):
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
    k_data = k_data[['cdate', 'open', 'high', 'close', 'low', 'volume', 'amount', 'outstanding', 'totals', 'adj', 'aprice', 'uprice', '60price']]
    k_data = k_data.rename(columns = {"cdate": "time"})

    i_data = i_data[['cdate', 'open', 'high', 'close', 'low', 'volume', 'amount']]
    i_data = i_data.rename(columns = {"cdate": "time"})
    #os.remove('k_data.json')
    #os.remove('d_data.json')
    #os.remove('i_data.json')
    return k_data, d_data, i_data

def movingaverage(x, N):
    return x.rolling(N).mean()

def ExpMovingAverage(values, window):
    weights = np.exp(np.linspace(-1., 0., window))
    weights /= weights.sum()
    a =  np.convolve(values, weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a

def computeMACD(x, slow=26, fast=12):
    """
    compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg'
    return value is emaslow, emafast, macd which are len(x) arrays
    """
    emaslow = ExpMovingAverage(x, slow)
    emafast = ExpMovingAverage(x, fast)
    return emaslow, emafast, emafast - emaslow

ax4 = None
d_data = None
priceMin = 0
volumeMin = 0

def onclick(event):
    if event.xdata is not None:
        tdate = mdates.num2date(event.xdata).strftime("%Y-%m-%d")
        tmp_df = d_data.loc[d_data.date == tdate]
        if tmp_df.empty:
            ax4.set_ylabel("%s no data" % tdate)
            ax4.grid(True, color = 'w')
        else:
            print(tmp_df[['date', 'sdate', 'price', 'volume']])
            ax4.clear()
            ax4.set_ylabel(tdate)
            ax4.barh(tmp_df.price, tmp_df.volume, height = 0.3, facecolor = 'lightskyblue', alpha = 1.0)
            ax4.grid(True, color = 'w')

def main():
    code = '601318'
    global ax4
    global d_data
    k_data, d_data, i_data = read_data(code)

    from mpl_finance import candlestick_ohlc
    # convert the datetime64 column in the dataframe
    k_data.time = pd.to_datetime(k_data.time, format='%Y-%m-%d')
    k_data.time = mdates.date2num(k_data.time)

    i_data.time = pd.to_datetime(i_data.time, format='%Y-%m-%d')
    i_data.time = mdates.date2num(i_data.time)

    fig = plt.figure(facecolor = '#07000d', figsize = (36, 36))
    mid = fig.canvas.mpl_connect("button_press_event", onclick)
    ax1 = plt.subplot2grid((6,6), (0,0), rowspan = 4 , colspan = 4, facecolor = '#07000d')

    candlestick_ohlc(ax1, k_data.values, width = 1, colorup = '#ff1717', colordown = '#53c156')

    #plot the average price
    ax1.plot(k_data.time, k_data['60price'], '#e1edf9', label = "60日成本均线", linewidth = 1.5)
    ax1.plot(k_data.time, k_data['uprice'], '#4ee6fd',  label = "无穷成本均线", linewidth = 1.5)

    priceMin = 0
    priceMax = k_data.high.values.max()

    ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.yaxis.label.set_color("w")
    ax1.set_ylabel("prices")
    ax1.set_ylim(priceMin, priceMax)
    ax1.grid(True, color = 'w')

    # plot the volume
    volumeMin = 0
    volumeMax = d_data.volume.values.max() 
    ax2 = plt.subplot2grid((6,6), (4, 0), rowspan = 1 , colspan = 4, facecolor = '#07000d', sharex = ax1)
    ax2.fill_between(k_data.time, volumeMin, k_data.volume, facecolor = '#00ffe8', alpha = 0.8)
    ax2.set_ylim(volumeMin, k_data.volume.values.max())
    ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.yaxis.label.set_color("w")
    ax2.set_ylabel("volumes")
    ax2.grid(True, color = 'w')

    #nema  = 9
    #nfast = 12
    #nslow = 26
    #emaslow, emafast, macd = computeMACD(k_data.close.values)
    #ema9 = ExpMovingAverage(macd, nema)

    # plot the index
    fillcolor = '#00ffe8'
    ax3 = plt.subplot2grid((6,6), (5, 0), rowspan = 1, colspan = 4, facecolor = '#07000d', sharex = ax1)
    ax3.yaxis.set_ticks_position('left')
    ax3.xaxis.set_ticks_position('bottom')
    candlestick_ohlc(ax3, i_data.values, width = 1, colorup = '#ff1717', colordown = '#53c156')
    ax3.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax3.set_ylabel("Shanghai Index")
    ax3.yaxis.label.set_color("w")
    ax3.grid(True, color = 'w')

    # plot the chip distribution on right
    ax4 = plt.subplot2grid((6,6), (0, 4), rowspan = 4, colspan = 2, facecolor = '#07000d', sharey = ax1)
    ax4.set_xlim(volumeMin, volumeMax)
    ax4.set_ylim(priceMin , priceMax)
    ax4.yaxis.label.set_color("w")
    ax4.grid(True, color = 'w')

    multi = MultiCursor(fig.canvas, (ax1, ax2, ax3), color='b', lw=1, horizOn = True, vertOn = True)
    plt.suptitle(code, color='w')
    plt.setp(ax1.get_xticklabels(), visible = True)
    plt.subplots_adjust(left=.09, bottom=.14, right=.94, top=.95, wspace=.20, hspace=0)
    plt.show()
    fig.canvas.mpl_disconnect(mid)
    plt.close(fig)

if __name__ == '__main__':
    main()
