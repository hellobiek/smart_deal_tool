# -*- coding: utf-8 -*-
"""
Created on Tue Oct 06 11:13:33 2015
提供两种数据调取方式，一种为系统自带画图，另一种提供array方式各数据的接口，详见cn.lib.pyalg_utils.py
@author: lenovo
"""
import pandas as pd
from pandas import DataFrame
from pyalgotrade import plotter, strategy
from pyalgotrade.technical import highlow
from pyalgotrade.stratanalyzer import returns
from matplotlib.pyplot import plot

import os
import sys
from os.path import abspath, dirname, join
sys.path.insert(0, dirname(dirname(abspath(__file__))))
from base.feed import dataFramefeed 

class PlateMomentumStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, N1, N2):
        super(PlateMomentumStrategy, self).__init__(feed)
        self.__instrument = instrument
        self.__feed = feed
        self.__position = None
        self.setUseAdjustedValues(False)
        self.__prices = feed[instrument].getPriceDataSeries()
        self.__high = highlow.High(self.__prices, N1, 3)
        self.__low = highlow.Low(self.__prices, N2, 3)
        self.__info = DataFrame(columns={'date', 'id', 'action', 'instrument', 'quantity', 'price'})  # 交易记录信息
        self.__info_matrix = []

    def addInfo(self, order):
        __date = order.getSubmitDateTime()  # 时间
        __action = order.getAction()        # 动作
        __id = order.getId()                # 订单号
        __instrument = order.getInstrument()# 股票
        __quantity = order.getQuantity()    # 数量
        __price = order.getAvgFillPrice()
        self.__info_matrix.append([__date, __id, __action, __instrument, __quantity, __price])

    # 有多重实现方式和存储方式，考虑到组合数据，最终选用dataFrame且ID默认，因为或存在一日多单
    def getInfo(self):
        _matrix = np.array(self.__info_matrix).reshape((len(self.__info_matrix), 6))
        return DataFrame({'date': _matrix[:, 0], 'id': _matrix[:, 1], 'action': _matrix[:, 2], 'instrument': _matrix[:, 3], 'quantity': _matrix[:, 4], 'price': _matrix[:, 5]})
        # 返回某一instrument的时间序列

    def getDateTimeSeries(self, instrument=None):  # 海龟交易法和vwamp方法不一样，一个instrument为数组，一个为值
        if instrument is None:
            return self.__feed[self.__instrument].getPriceDataSeries().getDateTimes()
        return self.__feed[instrument].getPriceDataSeries().getDateTimes()

    def getHigh(self):
        return self.__high

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("BUY at ￥%.2f" % (execInfo.getPrice()))
        self.addInfo(position.getEntryOrder())  # 在此处添加信息

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("SELL at ￥%.2f" % (execInfo.getPrice()))
        self.addInfo(position.getExitOrder())  # 在此处添加信息
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onBars(self, bars):
        # 若使用self__high[-1]这种值的话，不能是none,self.__high[0:0]为取前一日的  #也可以self.__high.__len__()！=3
        if self.__high[-1] == None:
            return

        if self.__high[-2] == None or self.__high[-3] == None:
            return

        bar = bars[self.__instrument]
        # If a position was not opened, check if we should enter a long position.
        # 如果不设定high的长度为3的话，可能取不到-3的值
        if self.__position is None or not self.__position.isOpen():
            # 判定今天价比昨日的最高价高，昨天价比前天的最高价低
            if self.__prices[-1] > self.__high[-2] and self.__prices[-2] < self.__high[-3]:
                shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
                # Enter a buy market order. The order is good till canceled.
                self.__position = self.enterLong(self.__instrument, shares, True)  # 多种实现方式，为记录信息简要写于一处
        elif not self.__position.exitActive() and self.__prices[-1] < self.__low[-2] and self.__prices[-2] > self.__low[-3]:
            # Check if we have to exit the position.
            self.__position.exitMarket()

def plate_momentum():
    # 从dataFrame中加载
    scode = "601318"
    feed = dataFramefeed.Feed()
    instrument = [scode]
    for code in instrument:
        filename = "/Volumes/data/quant/stock/data/tdx/history/days/1%s.csv" % code
        dat = pd.read_csv(filename, sep = ',')
        dat = dat.reset_index(drop = True)
        dat['adj close'] = dat['close']
        dat = dat.loc[(dat.date >= 20170101) & (dat.date <= 20180601)]
        dat['date'] = dat['date'].astype(str)
        dat['date'] = pd.to_datetime(dat.date).dt.strftime("%Y-%m-%d")
        dat = dat[['date', 'open', 'high', 'low', 'close', 'volume', 'adj close']]
        dat = dat.set_index('date')
        feed.addBarsFromDataFrame(code, dat)

    myStrategy = PlateMomentumStrategy(feed, scode, 20, 10)
    # Attach a returns analyzers to the strategy.
    returnsAnalyzer = returns.Returns()
    myStrategy.attachAnalyzer(returnsAnalyzer)

    # Attach the plotter to the strategy.
    plt = plotter.StrategyPlotter(myStrategy)

    # Plot the simple returns on each bar.
    plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())

    myStrategy.run()
    myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())

    plt.plot()

if __name__ == '__main__':
    plate_momentum()
