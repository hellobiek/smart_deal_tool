# -*- coding: utf-8 -*-
"""
ta-lib示例，示例包含使用原生talib,pyalgotrade自带talib，调用自己写的util.formular中的公式
"""
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import talib
import numpy as np
import pandas as pd
from pyalgotrade import strategy, plotter
from pyalgotrade.technical import ma, cross, macd
from pyalgotrade.stratanalyzer import returns, sharpe
from pyalgotrade.talibext import indicator
from talib import MA_Type
from algotrade.feed import dataFramefeed

class KDJBacktest(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, param, df):
        strategy.BacktestingStrategy.__init__(self, feed)
        # 自定义日线级别dataseries
        self.__instrument = instrument
        self.__feed = feed
        self.setUseAdjustedValues(False)
        self.__position = None
        self.__param  = param
        self.__kd = df
        self.__count = 0
        self.__prices = self.__feed[self.__instrument].getCloseDataSeries()
        self.__sma = ma.SMA(self.__prices, 5)
        self.__macd = macd.MACD(feed[instrument].getPriceDataSeries(), 12, 26, 9).getHistogram()

     def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("BUY at ￥%.2f" % (execInfo.getPrice()))

