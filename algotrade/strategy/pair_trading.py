# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import time
import traceback
import const as ct
import tushare as ts
from pandas import DataFrame
from algotrade.feed import dataFramefeed 
from common import add_suffix, get_tushare_client
from pyalgotrade import plotter, strategy, broker
import pyalgotrade.stratanalyzer.returns as sreturn
from pyalgotrade.stratanalyzer import sharpe, trades, drawdown

class PairTradingStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instruments, brk, beta, mean, std, cash):
        super(PairTradingStrategy, self).__init__(feed, brk)
        self.__feed = feed
        self.__position = None
        self.__instruments = instruments
        self.__info_dict = dict()
        self.__codex = instruments[0]
        self.__codey = instruments[1]
        self.__beta  = beta
        self.__mean  = mean
        self.__std   = std
        self.__cash  = cash
        for element in instruments:
            self.__info_dict[element] = DataFrame(columns={'date', 'id', 'action', 'quantity', 'price'})
        self.setUseAdjustedValues(False)

    def setInfo(self, order):
        __date = order.getSubmitDateTime()      # 时间
        __id = order.getId()                    # 订单
        __action = order.getAction()            # 动作
        __quantity = order.getQuantity()        # 数量
        __price = order.getAvgFillPrice()       # 均价
        __instrument = order.getInstrument()    # 股票
        self.__info_dict[__instrument].at[len(self.__info_dict)] = [__date, __id, __action, __quantity, __price]

    def getInfo(self, instrument):
        return self.__info_dict[instrument]

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
        self.__position.exitMarket()

    def onBars(self, bars):
        #总离场信号：买入后亏损达到10%后，立刻无脑止损
        if self.__cash / self.getBroker().getCash() < 0.9:
            if x_share > 0: self.__position = self.enterShort(self.__codex, x_share, True)
            if y_share > 0: self.__position = self.enterShort(self.__codey, y_share, True)
            return

        #每次对冲的多头头寸控制为当前持有现金的0.6
        x_price   = bars[self.__codex].getClose()
        x_share   = self.getBroker().getShares(self.__codex)
        x_extra   = bars[self.__codex].getExtraColumns()
        x_pchange = x_extra['pchange']

        y_price   = bars[self.__codey].getClose()
        y_share   = self.getBroker().getShares(self.__codey)
        y_extra   = bars[self.__codey].getExtraColumns()
        y_pchange = y_extra['pchange']

        spread = y_price - x_price * self.__beta
        score = (spread - self.__mean)/self.__std
        #X入场信号
        if score > 1 and x_share == 0:
            if y_share > 0: self.__position = self.enterShort(self.__codey, y_share, True)
            shares = 100 * int((self.getBroker().getCash()/(100 * x_price)) - 1)
            if shares > 0: self.__position = self.enterLong(self.__codex, shares, True)

        #Y入场信号
        if score < -1.1 and y_share == 0:
            if x_share > 0: self.__position = self.enterShort(self.__codex, x_share, True)
            shares = 100 * int((self.getBroker().getCash()/(100 * y_price) - 1))
            if shares > 0: self.__position = self.enterLong(self.__codey, shares, True)

        #离场的信号
        if score < 0.8 and score > -0.9:
            if x_share > 0: self.__position = self.enterShort(self.__codex, x_share, True)
            if y_share > 0: self.__position = self.enterShort(self.__codey, y_share, True) 

def main(mode = ct.PAPER_TRADING, start_date = '20180214', end_date = '20180628'):
    if mode == ct.PAPER_TRADING:
        cash = 100000
        beta = 9.49
        mean = -0.282
        std  = 34.73
        feed = dataFramefeed.Feed()
        instruments = ['300296', '300613']
        fpath       = '/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/tushare.json' 
        ts_client   = get_tushare_client(fpath)
        for code in instruments:
            df = ts.pro_bar(pro_api = ts_client, ts_code = add_suffix(code), adj = 'qfq', start_date = start_date, end_date = end_date)
            df = df.rename(columns = {"ts_code": "code", "trade_date": "date", "vol": "volume", "pct_change": "pchange"})
            df['date'] = df.date.apply(lambda x: time.strftime('%Y-%m-%d', time.strptime(x, "%Y%m%d")))
            df = df.set_index("date")
            feed.addBarsFromDataFrame(code, df)

        # broker setting
        # broker commission类设置
        broker_commission = broker.backtesting.TradePercentage(0.002)
        # fill strategy设置
        fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = 1.0)
        sli_stra = broker.slippage.NoSlippage()
        fill_stra.setSlippageModel(sli_stra)
        # 完善broker类
        brk = broker.backtesting.Broker(cash, feed, broker_commission)
        brk.setFillStrategy(fill_stra)

    pStrategy = PairTradingStrategy(feed, instruments, brk, beta, mean, std, cash)

    returnsAnalyzer = sreturn.Returns()
    pStrategy.attachAnalyzer(returnsAnalyzer)

    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    pStrategy.attachAnalyzer(sharpeRatioAnalyzer)

    drawDownAnalyzer = drawdown.DrawDown()
    pStrategy.attachAnalyzer(drawDownAnalyzer)

    tradesAnalyzer = trades.Trades()
    pStrategy.attachAnalyzer(tradesAnalyzer)

    plt = plotter.StrategyPlotter(pStrategy)

    # Plot the simple returns on each bar.
    plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())

    pStrategy.run()

    pStrategy.info("Final portfolio value: $%.2f" % pStrategy.getResult())
    pStrategy.info("Cumulative returns: %.2f %%" % (returnsAnalyzer.getCumulativeReturns()[-1] * 100))
    pStrategy.info("Sharpe ratio: %.2f" % (sharpeRatioAnalyzer.getSharpeRatio(0.05)))
    pStrategy.info("Max. drawdown: %.2f %%" % (drawDownAnalyzer.getMaxDrawDown() * 100))
    pStrategy.info("Longest drawdown duration: %s" % (drawDownAnalyzer.getLongestDrawDownDuration()))

    pStrategy.info("Final portfolio value: $%.2f" % pStrategy.getResult())

    pStrategy.info("Total trades: %d" % (tradesAnalyzer.getCount()))
    if tradesAnalyzer.getCount() > 0:
        profits = tradesAnalyzer.getAll()
        pStrategy.info("Avg. profit: $%2.f" % (profits.mean()))
        pStrategy.info("Profits std. dev.: $%2.f" % (profits.std()))
        pStrategy.info("Max. profit: $%2.f" % (profits.max()))
        pStrategy.info("Min. profit: $%2.f" % (profits.min()))
        returns = tradesAnalyzer.getAllReturns()
        pStrategy.info("Avg. return: %2.f %%" % (returns.mean() * 100))
        pStrategy.info("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        pStrategy.info("Max. return: %2.f %%" % (returns.max() * 100))
        pStrategy.info("Min. return: %2.f %%" % (returns.min() * 100))
    
    pStrategy.info("Profitable trades: %d" % (tradesAnalyzer.getProfitableCount()))
    if tradesAnalyzer.getProfitableCount() > 0:
        profits = tradesAnalyzer.getProfits()
        pStrategy.info("Avg. profit: $%2.f" % (profits.mean()))
        pStrategy.info("Profits std. dev.: $%2.f" % (profits.std()))
        pStrategy.info("Max. profit: $%2.f" % (profits.max()))
        pStrategy.info("Min. profit: $%2.f" % (profits.min()))
        returns = tradesAnalyzer.getPositiveReturns()
        pStrategy.info("Avg. return: %2.f %%" % (returns.mean() * 100))
        pStrategy.info("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        pStrategy.info("Max. return: %2.f %%" % (returns.max() * 100))
        pStrategy.info("Min. return: %2.f %%" % (returns.min() * 100))
    
    pStrategy.info("Unprofitable trades: %d" % (tradesAnalyzer.getUnprofitableCount()))
    if tradesAnalyzer.getUnprofitableCount() > 0:
        losses = tradesAnalyzer.getLosses()
        pStrategy.info("Avg. loss: $%2.f" % (losses.mean()))
        pStrategy.info("Losses std. dev.: $%2.f" % (losses.std()))
        pStrategy.info("Max. loss: $%2.f" % (losses.min()))
        pStrategy.info("Min. loss: $%2.f" % (losses.max()))
        returns = tradesAnalyzer.getNegativeReturns()
        pStrategy.info("Avg. return: %2.f %%" % (returns.mean() * 100))
        pStrategy.info("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        pStrategy.info("Max. return: %2.f %%" % (returns.max() * 100))
        pStrategy.info("Min. return: %2.f %%" % (returns.min() * 100))
    plt.plot()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        traceback.print_exc()
