# -*- coding: utf-8 -*-
"""
ta-lib示例，示例包含使用原生talib,pyalgotrade自带talib，调用自己写的util.formular中的公式
"""
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import talib
import traceback
import const as ct
import numpy as np
import pandas as pd
from cstock import CStock
from cindex import CIndex
from pyalgotrade import strategy, plotter
from pyalgotrade.technical import ma, cross, macd
from pyalgotrade.stratanalyzer import returns, sharpe
from pyalgotrade.talibext import indicator
from talib import MA_Type
from algotrade.feed import dataFramefeed
class KDJStrategy(strategy.BacktestingStrategy):
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

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("SELL at ￥%.2f" % (execInfo.getPrice()))
        self.__position = None

    def checkKDJ(self, bars):
        signal = 0
        # this time we can use k[-1]'s data
        ##########################  using talib
        # If a position was not opened, check if we should enter a long position.
        # self.__stoch = indicator.STOCH(self.__feed[self.__instrument],len(self.__feed[self.__instrument]),
        #                               self.__param[0],self.__param[1],self.__param[2],self.__param[3],self.__param[4])
        # self.__k,self.__d = self.__stoch[0],self.__stoch[1]

        ########################## using utils.formular for kdj:this is true
        kd = self.__kd[['KDJ_K', 'KDJ_D']][self.__kd.index <= bars.getDateTime()].tail(2)
        self.__k, self.__d = kd.KDJ_K.values, kd.KDJ_D.values
        if self.__k.__len__() < 2:
            return signal
        #########################
        if cross.cross_above(self.__k, [self.__param[6], self.__param[6]]) > 0 \
            or cross.cross_above(self.__k,   [self.__param[5],self.__param[5]]) > 0:  # 得取其high前一天的值cross 参数为2，所以high中至少应当有3个值
            signal = 1
        elif cross.cross_below(self.__k, [self.__param[6],self.__param[6]]) > 0 \
            or cross.cross_below(self.__k, [self.__param[5],self.__param[5]]) > 0:
            signal = -1
        return signal

    def checkAroon(self, bars):
        signal = 0
        self.__aroondown,self.__aroonup = indicator.Adx(self.__feed[self.__instrument],len(self.__feed[self.__instrument]),20)
        self.__aroon = self.__aroonup[-2:] - self.__aroondown[-2:]
        if self.__aroon is None or np.isnan(self.__aroon[0]):  # to check the first item is Nan
            return signal
        if cross.cross_above(self.__aroon,[15,15]):
            signal = 1
        elif cross.cross_below(self.__aroon,[15,15]):
            signal = -1
        return signal

    def onBars(self, bars):
        self.__count += 1
        signal = self.checkkdj(bars)
        #if self.__count > 2530 :
        #    print bars.getDateTime(), self.__k[-1], self.__d[-1], bars[self.__instrument].getClose()
        if self.__position is None or not self.__position.isOpen():
            if signal == 1:
                shares = 100 * (int(self.getBroker().getCash() * 0.7 / bars[self.__instrument].getPrice()) / 100)
                self.__position = self.enterLong(self.__instrument, shares, True)
                #print "BUY:", self.__prices[-1], self.getBroker().getShares(self.__instrument), shares, self.__prices[-1] * shares / self.getBroker().getCash(), self.getBroker().getCash()
        elif not self.__position.exitActive():
            if signal == -1:
                self.__position.exitMarket()
                #print "SELL: ", self.__prices[-1], self.getBroker().getShares(self.__instrument), self.getBroker().getCash()

def main(code = '000001'):
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data()
    data = data.set_index('date')
    feed = dataFramefeed.Feed()
    feed.addBarsFromDataFrame(code, data)
    # broker setting
    # broker commission类设置
    broker_commission = broker.backtesting.TradePercentage(0.01)
    # fill strategy设置
    fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = 0.001)
    sli_stra = broker.slippage.NoSlippage()
    fill_stra.setSlippageModel(sli_stra)
    # 完善broker类
    brk = broker.backtesting.Broker(threshold * len(instruments), feed, broker_commission)
    brk.setFillStrategy(fill_stra)
    # 设置strategy
    myStrategy = KDJStrategy(feed, code, param = param, df = formular.KDJ(dat,14,3,3))
    # Attach a returns analyzers to the strategy
    returnsAnalyzer = returns.Returns()
    myStrategy.attachAnalyzer(returnsAnalyzer)
    # Attach a sharpe ratio analyzers to the strategy
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    myStrategy.attachAnalyzer(sharpeRatioAnalyzer)
    # Attach the plotter to the strategy
    plt = plotter.StrategyPlotter(myStrategy, True, True, True)
    plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())
    # Run Strategy 
    myStrategy.run()
    myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())
    plt.plot()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        traceback.print_exc()
