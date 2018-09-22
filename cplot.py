from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY, YEARLY
from matplotlib.dates import MonthLocator, MONTHLY
from mpl_finance import candlestick_ohlc
import datetime as dt
import pylab

MA1 = 10
MA2 = 50

stock_b_code = '601318'
def readstkData():
    with open('data.json', 'r') as f:
        data = pd.read_json(f.read(), orient='records', lines=True)
    data = data[['24price', '60price', '8price', 'adj', 'amount', 'aprice', 'cdate', 'close', 'high', 'low', 'open', 'outstanding', 'totals', 'uprice', 'volume']]
    data = data[['cdate', 'open', 'high', 'low', 'close', '24price', '60price', '8price', 'adj', 'amount', 'aprice', 'outstanding', 'totals', 'uprice', 'volume']]
    data = data.rename(columns={"cdate": "time"})
    return data

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

def main():
    data = readstkData()

    # convert the datetime64 column in the dataframe to 'float days'
    data.time = pd.to_datetime(data.time, format='%Y-%m-%d')
    data.time = mdates.date2num(data.time)

    Av1 = movingaverage(data.close, MA1)
    Av2 = movingaverage(data.close, MA2)

    SP = len(data.time.values[MA2-1:])


    fig = plt.figure(facecolor = '#07000d', figsize = (15,10))
    ax1 = plt.subplot2grid((6,4), (1,0), rowspan = 4, colspan = 4, facecolor = '#07000d')
    candlestick_ohlc(ax1, data.values[-SP:], width=.6, colorup = '#ff1717', colordown = '#53c156')
    label1 = str(MA1) + ' SMA'
    label2 = str(MA2) + ' SMA'
 
    ax1.plot(data.time.values[-SP:], Av1[-SP:], '#e1edf9', label = label1, linewidth = 1.5)
    ax1.plot(data.time.values[-SP:], Av2[-SP:], '#4ee6fd', label = label2, linewidth = 1.5)
    ax1.grid(True, color='w')
    ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.yaxis.label.set_color("w")
 
    volumeMin = 0
    ax1v = ax1.twinx()
    ax1v.fill_between(data.time.values[-SP:], volumeMin, data.volume.values[-SP:], facecolor='#00ffe8', alpha=.4)
    ax1v.axes.yaxis.set_ticklabels([])
    ax1v.grid(False)

    ###Edit this to 2, so it's a bit larger
    ax1v.set_ylim(0, 4 * data.volume.values.max())

    ax1.spines['bottom'].set_color("#5998ff")
    ax1.spines['top'].set_color("#5998ff")
    ax1.spines['left'].set_color("#5998ff")
    ax1.spines['right'].set_color("#5998ff")
    ax1.tick_params(axis='y', colors='w')
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax1.tick_params(axis='x', colors='w')
    plt.ylabel('Stock price and Volume')

    # plot an MACD indicator on bottom
    ax2 = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
    fillcolor = '#00ffe8'
    nslow = 26
    nfast = 12
    nema = 9
    emaslow, emafast, macd = computeMACD(data.close.values)

    ema9 = ExpMovingAverage(macd, nema)

    ax2.plot(data.time.values[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
    ax2.plot(data.time.values[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
    ax2.fill_between(data.time.values[-SP:], macd[-SP:] - ema9[-SP:], 0, alpha = 0.5, facecolor = fillcolor, edgecolor = fillcolor)
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))

    ax2.spines['bottom'].set_color("#5998ff")
    ax2.spines['top'].set_color("#5998ff")
    ax2.spines['left'].set_color("#5998ff")
    ax2.spines['right'].set_color("#5998ff")
    ax2.tick_params(axis = 'x', colors = 'w')
    ax2.tick_params(axis = 'y', colors = 'w')
    plt.ylabel('MACD', color = 'w')
    ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins = 5, prune = 'upper'))
    for label in ax2.xaxis.get_ticklabels():
        label.set_rotation(45)

    plt.suptitle(stock_b_code, color = 'w')
    plt.setp(ax1.get_xticklabels(), visible = False)

    plt.subplots_adjust(left = .09, bottom = .14, right = .94, top = .95, wspace = .20, hspace = 0)
    plt.show()

if __name__ == '__main__':
    main()
