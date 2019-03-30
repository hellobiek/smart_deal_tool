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
from common import is_df_has_unexpected_data, resample
from cstock import CStock
from cindex import CIndex
from pyalgotrade import strategy, plotter, broker
from pyalgotrade.technical import ma, cross, macd
from pyalgotrade.stratanalyzer import returns, sharpe
from pyalgotrade.talibext import indicator
from algotrade.technical.kdj import kdj
from algotrade.technical.ma import ma
from algotrade.feed import dataFramefeed
'''
KDJ模型的策略
短线指标:日线
    买入条件
       KDJ的K指标和D指标20以下，K线向上穿越D线
       价格站上5日线
       成交量超过5日均量1.5倍
       机构资金净流入(L2数据)
    卖出条件
        止盈条件
            KD指标死叉
            价格跌破5日线
            成交量超过5日均量1.5倍
        止损条件
            亏损5%
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
'''
class KDJStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, param, df):
        strategy.BacktestingStrategy.__init__(self, feed)
        self.__instrument = instrument
        self.__feed = feed
        self.setUseAdjustedValues(False)
        self.__position = None
        self.__param  = param
        self.__data = df
        self.__price_ma_s = df[['ma_%s' % param['ma_s']]]
        self.__price_ma_m = df[['ma_%s' % param['ma_m']]]
        self.__price_ma_l = df[['ma_%s' % param['ma_l']]]
        self.__volume_ma = df[['ma_%s' % param['volume']]]
        self.__prices = self.__feed[self.__instrument].getCloseDataSeries()

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        #self.info("%s buy at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        #self.info("%s sell at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))
        self.__position = None

    def checkMA(self, bars):
        signal = 0
        data = self.__data
        d_s = data['ma_5'][self.__data.index == bars.getDateTime()]
        if len(d_s) <= 0: return signal
        d_m = data['ma_10'][self.__data.index == bars.getDateTime()]
        d_l = data['ma_20'][self.__data.index == bars.getDateTime()]
        d_array = [abs(d_m - d_s), abs(d_l - d_s), abs(d_l - d_m)]
        score = sum(abs(d_array - np.mean(d_array))) * np.std(d_array)
        #self.debug("date %s ma score:%s" % (bars.getDateTime(), score))
        return score

    def checkPrice(self, bars):
        signal = 0
        price_data = self.__data[['close', 'ma_5']][self.__data.index <= bars.getDateTime()].tail(2)
        price, ma_price = price_data.close.values, price_data.ma_5.values
        if len(price) < 2: return signal
        if cross.cross_above(price, ma_price) > 0:
            signal = 1
        elif cross.cross_below(price, ma_price) > 0:
            signal = -1
        return signal

    def checkKDJ(self, bars):
        signal = 0
        kd = self.__data[['k', 'd']][self.__data.index <= bars.getDateTime()].tail(2)
        k_value, d_value = kd.k.values, kd.d.values
        if len(k_value) < 2: return signal
        if cross.cross_above(k_value, d_value) > 0 and k_value[1] < self.__param['lthreshold'] and d_value[1] < self.__param['lthreshold']:
            ma_signal = self.checkMA(bars)
            self.info("buy at ￥%s, K value %s, D value %s MA score %s" % (bars.getDateTime(), k_value[1], d_value[1], ma_signal))
            signal = 1
        #elif k_value[1] > self.__param['hthreshold'] and d_value[1] > self.__param['hthreshold']:
        elif k_value[1] > self.__param['lthreshold'] and d_value[1] > self.__param['lthreshold']:
            signal = -1
        return signal

    def checkVolume(self, bars):
        signal = 0
        volume_data = self.__data[['volume', 'volume_ma_5']][self.__data.index <= bars.getDateTime()].tail(2)
        volume, ma_volume = volume_data.volume.values, volume_data.volume_ma_5.values
        if len(volume) < 2: return signal
        if cross.cross_above(volume, ma_volume) > 0:
            signal = 1
        elif cross.cross_below(volume, ma_volume) > 0:
            signal = -1
        return signal

    def checkSignal(self, bars):
        kdj_signal = self.checkKDJ(bars)
        price_signal = self.checkPrice(bars)
        volume_signal = self.checkVolume(bars)
        if kdj_signal == 1:
            return 1
        if kdj_signal == -1:
            return -1
        return 0

    def onBars(self, bars):
        signal = self.checkSignal(bars)
        if self.__position is None or not self.__position.isOpen():
            if signal == 1:
                shares = int(self.getBroker().getCash() / bars[self.__instrument].getPrice())
                self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            if signal == -1:
                self.__position.exitMarket()

def main():
    code = '000001'
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data_in_range(start_date = '2001-01-01', end_date ='2019-01-01')
    data = data.set_index('date')
    if is_df_has_unexpected_data(data):
        print("data illlegal")
        return
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
    cash = 100000
    brk = broker.backtesting.Broker(100000, feed, broker_commission)
    brk.setFillStrategy(fill_stra)
    # 设置strategy
    param = {'lthreshold': 20, 'hthreshold': 80, 'unit': 'D', 'ma_s': 5, 'ma_m': 10, 'ma_l': 20, 'volume': 5}
    data.index = pd.to_datetime(data.index)
    data = kdj(data)
    data = ma(data, param['ma_s'])
    data = ma(data, param['ma_m'])
    data = ma(data, param['ma_l'])
    data = ma(data, param['volume'], key = 'volume', name = 'volume_ma')
    data = data.dropna(how='any')
    myStrategy = KDJStrategy(feed, code, param, data)
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
