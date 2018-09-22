#coding=utf-8
import numpy as np
import pandas as pd
from pandas import DataFrame, Series

import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from matplotlib.widgets import MultiCursor
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY, YEARLY
from matplotlib.dates import MonthLocator, MONTHLY
from mpl_finance import candlestick_ohlc
def read_data():
    with open('data.json', 'r') as f: data = pd.read_json(f.read(), orient='records', lines=True)
    data = data[['cdate', 'open', 'high', 'low', 'close', '24price', '60price', '8price', 'adj', 'amount', 'aprice', 'outstanding', 'totals', 'uprice', 'volume']]
    return data.rename(columns={"cdate": "time"})

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

def onclick(event):
    print('button=%d, x=%d, y=%d, xdata=%f, ydata=%f' % (event.button, event.x, event.y, event.xdata, event.ydata))

def main():
    MA1 = 5
    MA2 = 20
    code = '601318'
    data = read_data()
    # convert the datetime64 column in the dataframe
    data.time = pd.to_datetime(data.time, format='%Y-%m-%d')
    data.time = mdates.date2num(data.time)

    av1 = movingaverage(data.close, MA1)
    av2 = movingaverage(data.close, MA2)
    SP = len(data.time.values[MA2-1:])

    fig = plt.figure(facecolor = '#07000d', figsize = (36, 36))
    cid = fig.canvas.mpl_connect('button_press_event', onclick)
    ax1 = plt.subplot2grid((6,6), (0,0), rowspan = 4 , colspan = 4, facecolor = '#07000d')
    candlestick_ohlc(ax1, data.values[-SP:], width = 1, colorup = '#ff1717', colordown = '#53c156')
    label1 = str(MA1) + ' SMA'
    label2 = str(MA2) + ' SMA'
 
    # plot the price
    ax1.plot(data.time.values[-SP:], av1[-SP:], '#e1edf9', label = label1, linewidth = 1.5)
    ax1.plot(data.time.values[-SP:], av2[-SP:], '#4ee6fd', label = label2, linewidth = 1.5)
    ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.yaxis.label.set_color("w")
    ax1.set_ylabel("prices")
    ax1.set_ylim(data.low.values.min(), data.high.values.max())
    ax1.grid(True, color = 'w')

    # plot the volume
    volumeMin = 0
    ax2 = plt.subplot2grid((6,6), (4, 0), rowspan = 1 , colspan = 4, facecolor = '#07000d', sharex = ax1)
    ax2.fill_between(data.time.values[-SP:], volumeMin, data.volume.values[-SP:], facecolor = '#00ffe8', alpha = 0.8)
    ax2.set_ylim(volumeMin, data.volume.values.max())
    ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.yaxis.label.set_color("w")
    ax2.set_ylabel("volumes")
    ax2.grid(True, color = 'w')

    nema  = 9
    nfast = 12
    nslow = 26
    emaslow, emafast, macd = computeMACD(data.close.values)
    ema9 = ExpMovingAverage(macd, nema)
    fillcolor = '#00ffe8'

    # plot an MACD indicator on bottom
    ax3 = plt.subplot2grid((6,6), (5, 0), rowspan = 1, colspan = 4, facecolor = '#07000d', sharex = ax1)
    ax3.plot(data.time.values[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
    ax3.plot(data.time.values[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
    ax3.fill_between(data.time.values[-SP:], macd[-SP:] - ema9[-SP:], 0, alpha = 0.5, facecolor = fillcolor, edgecolor = fillcolor)
    ax3.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax3.set_ylabel("MACD")
    ax3.yaxis.label.set_color("w")
    ax3.grid(True, color = 'w')

    ax4 = plt.subplot2grid((6,6), (0, 4), rowspan = 6, colspan = 2, facecolor = '#07000d')
    ax4.grid(True, color = 'w')

    multi = MultiCursor(fig.canvas, (ax1, ax2, ax3), color='b', lw=1, horizOn = True, vertOn = True)
    fig.canvas.mpl_disconnect(cid)
    plt.suptitle(code, color='w')
    plt.setp(ax1.get_xticklabels(), visible = True)
    plt.subplots_adjust(left=.09, bottom=.14, right=.94, top=.95, wspace=.20, hspace=0)
    plt.show()
    plt.close(fig)

if __name__ == '__main__':
    main()
