# -*- coding: utf-8 -*-
'''
http://finance.sina.com.cn/stock/stocklearnclass/20130119/171814340618.shtml?bsh_bid=184197665
KDJ模型的策略
短线指标:日线
    买入条件
       KDJ的K指标和D指标20以下，K线向上穿越D线
       价格站上5日线
       放量上涨3%以上
       成交量超过5日均量1.5倍
    卖出条件
        止盈条件: 盈利超过50%
        止损条件: 亏损超过10%
中线指标:周线
    买入条件
        KDJ的K指标和D指标20以下，K线向上穿越D线
        价格站上5周线，且5周线金叉20周线
        资金净流入(L2数据)
    卖出条件
月线指标:月线    
    买入条件
        KDJ的K指标和D指标20以下，K线向上穿越D线(日线，周线和月线同时20以下)，大胆买入。
    卖出条件
        等到盈利，无止损
结论：对于长线而言，是很靠谱的。对于短线，很不靠谱，需要使用机器学习的算法来集成。
'''
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import itertools
import traceback
import const as ct
import numpy as np
import pandas as pd
from pyalgotrade.technical import ma
from pyalgotrade.technical import cross 
from pyalgotrade.optimizer import local
from pyalgotrade import strategy, plotter, broker
from pyalgotrade.stratanalyzer import returns, sharpe
from cstock import CStock
from cindex import CIndex
#from algotrade.technical.ma import ma
from algotrade.technical.kdj import kdj
from algotrade.feed import dataFramefeed
from common import is_df_has_unexpected_data, resample
class KDJStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, brk, df, instrument, lthreshold, hthreshold, max_loss, min_profit, volume_period, price_peried, largest_pchange, volume_threshold):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__data = df
        self.__position = None
        self.__max_loss = max_loss
        self.__min_profit = min_profit
        self.__instrument = instrument
        self.__low_threshold = lthreshold
        self.__high_threshold = hthreshold
        self.__largest_pchange = largest_pchange
        self.__volume_threshold = volume_threshold
        self.prices = feed[instrument].getPriceDataSeries()
        self.sma_price = ma.SMA(self.prices, price_peried)
        self.volumes = feed[instrument].getVolumeDataSeries()
        self.sma_volume = ma.SMA(self.volumes, volume_period)
        self.setUseAdjustedValues(False)

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        self.info("%s buy at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("%s sell at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))
        self.info("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
        self.__position = None

    def checkVolume(self, bars):
        if self.sma_volume[-1] is None: return 0
        return 1 if self.volumes[-1] / self.sma_volume[-1] > self.__volume_threshold else 0

    def checkPrice(self, bars):
        signal = 0
        price_data = self.__data[['pchange']][self.__data.index == bars.getDateTime()]
        if len(price_data) < 1: return signal
        pchange = price_data.pchange.values
        return 1 if pchange[0] > self.__largest_pchange and self.prices[-1] > self.sma_price[-1] else 0

    def checkProfit(self):
        if self.__position is None: return 0
        profit = self.__position.getReturn()
        return 1 if profit < self.__max_loss or profit > self.__min_profit else 0

    def checkSignal(self, bars):
        kdj_signal = self.checkKDJ(bars)
        price_signal = self.checkPrice(bars)
        volume_signal = self.checkVolume(bars)
        #if kdj_signal == 1 and volume_signal == 1 and price_signal == 1:
        if kdj_signal == 1:
            return 1
        #if self.checkProfit() == 1:
        if kdj_signal == -1:
            return -1
        return 0

    def checkKDJ(self, bars):
        signal = 0
        kd = self.__data[['k', 'd']][self.__data.index <= bars.getDateTime()].tail(2)
        k_value, d_value = kd.k.values, kd.d.values
        if len(k_value) < 2: return signal
        if cross.cross_above(k_value, d_value) > 0 and k_value[1] < self.__low_threshold and d_value[1] < self.__low_threshold:
            signal = 1
        elif k_value[1] < self.__high_threshold:
            signal = -1
        return signal

    def onBars(self, bars):
        signal = self.checkSignal(bars)
        if self.__position is None or not self.__position.isOpen():
            if signal == 1:
                shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
                self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            if signal == -1:
                self.__position.exitMarket()

def gen_broker(feed, cash = 10000000, trade_percent = 0.01, volume_limit = 0.01):
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

def get_data(code, start_date, end_date):
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data_in_range(start_date, end_date)
    data = data.set_index('date')
    if is_df_has_unexpected_data(data): return None
    data.index = pd.to_datetime(data.index)
    data = kdj(data)
    data = data.dropna(how='any')
    return data

def parameters_generator(code, brk, data):
    brks = [brk]
    datas = [data]
    instrument = [code]
    lthreshold = [5  + x * 0.1 for x in range(0,31)]
    hthreshold = [75 + x * 0.1 for x in range(0,21)]
    max_loss = [-0.15 + x * 0.01 for x in range(0,11)]
    min_profit = [0.1 + x * 0.1 for x in range(0,100)]
    volume_period = [5 + x for x in range(6)]
    price_period = [5 + x for x in range(6)]
    largest_pchange = [1 + x * 0.1 for x in range(0, 31)]
    volume_threshold = [1 + x * 0.1 for x in range(0, 6)]
    return itertools.product(brks, datas, instrument, lthreshold, hthreshold, max_loss, min_profit, volume_period, price_period, largest_pchange, volume_threshold)

def grid_search(code, start_date, end_date):
    data = get_data(code, start_date, end_date)
    if data is None: return
    feed = dataFramefeed.Feed()
    feed.addBarsFromDataFrame(code, data)
    brk = gen_broker(feed)
    local.run(KDJStrategy, feed, parameters_generator(code, brk, data), workerCount = 2)

def main(code, start_date, end_date):
    data = get_data(code, start_date, end_date)
    feed = dataFramefeed.Feed()
    feed.addBarsFromDataFrame(code, data)
    # Set Strategy
    brk = gen_broker(feed)
    myStrategy = KDJStrategy(feed, brk, data, code, 45, 100, -0.05, 0.1, 5, 5, 1.5, 1.2)
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
        grid_search(code, start_date, end_date)
        #main(code, start_date, end_date)
    except Exception as e:
        traceback.print_exc()
