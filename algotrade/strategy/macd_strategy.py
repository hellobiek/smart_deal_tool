# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import pandas as pd
from cstock import CStock
from common import is_df_has_unexpected_data
from algotrade.feed import dataFramefeed
from algotrade.technical.ma import MACD
from algotrade.indicator.macd import Macd
from algotrade.strategy import gen_broker
from pyalgotrade import strategy, plotter, broker
from pyalgotrade.stratanalyzer import returns, sharpe
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, old_macd, feed, brk, signal_period_unit, fastEMA, slowEMA, signalEMA, maxLen, instrument):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__position = None
        self.__instrument = instrument
        self.__signal_period_unit = signal_period_unit
        self.__macd = Macd(old_macd, feed, fastEMA, slowEMA, signalEMA, maxLen, instrument)
        self.setUseAdjustedValues(False)

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("%s buy at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("%s sell at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))
        self.__position = None

    def onBars(self, bars):
        #if self.__position is None or not self.__position.isOpen():
        #    shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
        #    self.__position = self.enterLong(self.__instrument, shares, True)
        #elif not self.__position.exitActive():
        #    self.__position.exitMarket()
        pass

def get_feed(code, start_date, end_date, peried):
    feed = dataFramefeed.Feed()
    for code in codes:
        cstock_obj = CStock(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        data = cstock_obj.get_k_data_in_range(start_date, end_date)
        data = data.set_index('date')
        if is_df_has_unexpected_data(data): return None, None
        data.index = pd.to_datetime(data.index)
        data = data.dropna(how='any')
        macd_old = MACD.macd(data)
        feed.addBarsFromDataFrame(code, data)
    return feed, macd_old

MID = 9
LONG = 26
SHORT = 12
DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM = 250
def main(codes, start_date, end_date, maxLen = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM, signal_period_unit = 5, peried = 'D'):
    '''
    count: 采用过去count个bar内极值的最大值作为参考。
    signal_period_unit: 检测信号的时间间隔。与信号检测的周期保持一致。
    '''
    feed, macd_old = get_feed(codes, start_date, end_date, peried)
    if feed is None: return
    # Set Strategy
    brk = gen_broker(feed)
    for code in codes:
        myStrategy = MACDStrategy(macd_old, feed, brk, signal_period_unit, SHORT, LONG, MID, maxLen, code)
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
        start_date = '2001-01-01'
        end_date   = '2019-03-10'
        #codes = ['002466', '601398']  # 股票池
        codes = ['601318']  # 股票池
        main(codes, start_date, end_date)
    except Exception as e:
        traceback.print_exc()
