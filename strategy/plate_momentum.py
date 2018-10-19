# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from cmysql import CMySQL
from cstock import CStock
from pandas import DataFrame
from technical import bfp, gkr, prt, pvh, rat, rolling_peak
from base.feed import dataFramefeed 
from pyalgotrade.technical import highlow, ma
from pyalgotrade.stratanalyzer import returns
from pyalgotrade import plotter, strategy, broker

class PlateMomentumStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instruments, brk, threshold):
        super(PlateMomentumStrategy, self).__init__(feed, brk)
        self.__feed = feed
        self.__position = None
        self.__instruments = instruments
        self.__sma = dict()
        self.__info_dict = dict()
        self.__threshold = dict()
        for element in instruments:
            self.__threshold[element] = threshold
            self.__sma[element] = ma.SMA(feed[element].getCloseDataSeries(), 15)
            self.__info_dict[element] = DataFrame(columns={'date', 'id', 'action', 'quantity', 'price'})
        self.setUseAdjustedValues(False)

    def setInfo(self, order):
        __date = order.getSubmitDateTime()      # 时间
        __action = order.getAction()            # 动作
        __id = order.getId()                    # 订单号
        __instrument = order.getInstrument()    # 股票
        __quantity = order.getQuantity()        # 数量
        __price = order.getAvgFillPrice()
        self.__info_dict[__instrument].at[len(self.__info_dict)] = [__date, __id, __action, __quantity, __price]

    def getInfo(self, instrument):
        return self.__info_dict[instrument]

    def getThreshold(self, instrument):
        return self.__threshold[instrument]

    def getDateTimeSeries(self, instrument = None):
        return self.__feed[instrument].getPriceDataSeries().getDateTimes()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onBars(self, bars):
        for element in bars.getInstruments():
            price = bars[element].getClose()
            volume = bars[element].getVolume()
            if self.__sma[element][-1] is None: continue
            price = bars[element].getClose()
            if self.__position is None:
                if price < self.__sma[element][-1] * 0.9:
                    shares = 100 * int(self.getBroker().getCash()/(100 * price))
                    self.info("before buy %s %s at ￥%.2f, exists cash:%s" % (element, shares, price, self.getBroker().getCash()))
                    self.__position = self.enterLong(element, shares, True)
                    self.info("after buy %s %s at ￥%.2f, exists cash:%s" % (element, shares, price, self.getBroker().getCash()))
            else:
                if price > self.__sma[element][-1] * 1.1:
                    if self.__position is not None and not self.__position.exitActive():
                        self.info("before sell %s %s at ￥%.2f, exists cash:%s" % (element, self.getBroker().getShares(element), price, self.getBroker().getCash()))
                        self.__position.exitMarket()
                        self.info("after sell %s %s at ￥%.2f, exists cash:%s" % (element, self.getBroker().getShares(element), price, self.getBroker().getCash()))
 
def choose_stock():
    return ['002153']

def plate_momentum(mode = ct.PAPER_TRADING, start_date = '2018-03-01', end_date = '2018-10-28'):
    if mode == ct.PAPER_TRADING:
        threshold = 100000
        feed = dataFramefeed.Feed()
        instruments = choose_stock()
        for code in instruments:
            data = CStock(code, should_create_influxdb = False, should_create_mysqldb = False).get_k_data_in_range(start_date, end_date)
            data = data.set_index('date')
            feed.addBarsFromDataFrame(code, data)

        # broker setting
        # broker commission类设置
        broker_commission = broker.backtesting.TradePercentage(0.002)
        # fill strategy设置
        #fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = 1.0)
        #sli_stra = broker.slippage.NoSlippage()
        #fill_stra.setSlippageModel(sli_stra)
        # 完善broker类
        brk = broker.backtesting.Broker(threshold * len(instruments), feed, broker_commission)
        #brk.setFillStrategy(fill_stra)

    myStrategy = PlateMomentumStrategy(feed, instruments, brk, threshold)

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
