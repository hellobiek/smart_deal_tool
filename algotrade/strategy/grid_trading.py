# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import numpy as np
import pandas as pd
from cindex import CIndex
from algotrade.feed import dataFramefeed
from common import is_df_has_unexpected_data
from pyalgotrade.technical import ma
from pyalgotrade import strategy, plotter, broker
from pyalgotrade.stratanalyzer import returns, sharpe
class GridSearchStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, brk, instrument, price_peried, weights, bands):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        assert(len(bands) + 1 == len(weights))
        self.__bands = bands
        self.__weights = weights
        self.__instrument = instrument
        self.__prices = feed[instrument].getPriceDataSeries() 
        self.__sma_prices = ma.SMA(self.__prices, price_peried)
        self.setUseAdjustedValues(False)

    def getPoitionRaito(self, ratio):
        for rindex in range(len(self.__bands)):
            if ratio < self.__bands[rindex]:
                return self.__weights[rindex]
        return self.__weights[rindex + 1]

    def getExpectPosition(self, bars):
        price = bars[self.__instrument].getClose()
        ratio = (price - self.__sma_prices[-1]) / price
        return self.getPoitionRaito(ratio)

    def getActualPosition(self):
        return 1.0 - (self.getBroker().getCash() / self.getBroker().getEquity())

    def getAction(self, bars):
        expect_position = self.getExpectPosition(bars)
        actial_position = self.getActualPosition()
        price = bars[self.__instrument].getClose() 
        if expect_position > actial_position:
            cash = self.getBroker().getEquity() * (expect_position - actial_position)
            return 1, self.__instrument, int((cash * 0.91) / price)
        elif expect_position < actial_position:
            cash = self.getBroker().getEquity() * (actial_position - expect_position)
            return -1, self.__instrument, int(cash / price) 
        else:
            return 0, self.__instrument, 0

    def onBars(self, bars):
        if self.__sma_prices[-1] is None: return
        action, instrument, shares = self.getAction(bars)
        if action != 0: self.marketOrder(instrument, action * shares)

def getData(code, start_date, end_date):
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data_in_range(start_date, end_date)
    data = data.set_index('date')
    if is_df_has_unexpected_data(data): return None
    data.index = pd.to_datetime(data.index)
    data = data.dropna(how='any')
    return data

def genBroker(feed, cash = 10000000, trade_percent = 0.01, volume_limit = 0.01):
    # cash：初始资金
    # trade_percent: 手续费, 每笔交易金额的百分比
    # volume_limit: 每次交易能成交的量所能接受的最大比例
    # Broker Setting
    # Broker Commission类设置
    broker_commission = broker.backtesting.TradePercentage(trade_percent)
    # Fill Strategy设置
    fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = volume_limit)
    sli_stra = broker.slippage.NoSlippage()
    fill_stra.setSlippageModel(sli_stra)
    # 完善Broker类
    brk = broker.backtesting.Broker(cash, feed, broker_commission)
    brk.setFillStrategy(fill_stra)
    return brk

def main(code, start_date, end_date):
    data = getData(code, start_date, end_date)
    if data is None: return
    feed = dataFramefeed.Feed()
    feed.addBarsFromDataFrame(code, data)
    # Set Strategy
    brk = genBroker(feed)
    bands   = [-0.40, -0.30, -0.20, -0.10, 0.10, 0.20, 0.30, 0.40]
    weights = [ 1.00,  0.90,  0.70,  0.60, 0.50, 0.35, 0.25, 0.05, 0.00]
    #base_price = np.mean(data.close) * 0.95
    base_day = 10
    myStrategy = GridSearchStrategy(feed, brk, code, base_day, weights, bands)
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
        code = '000001'
        start_date = '2001-01-01'
        end_date = '2019-03-10'
        main(code, start_date, end_date)
    except Exception as e:
        traceback.print_exc()
