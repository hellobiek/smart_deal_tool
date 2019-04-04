# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
from algotrade.feed import dataFramefeed
from algotrade.indicator.macd import MacdCache
from pyalgotrade import strategy, plotter, broker
from pyalgotrade.strategy import gen_broker, get_data
from pyalgotrade.stratanalyzer import returns, sharpe
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, brk):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__position = None
        self.setUseAdjustedValues(False)

    def initialize(self):
        pass

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
        if self.__position is None or not self.__position.isOpen():
            shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
            self.__position = self.enterLong(self.__instrument, shares, True)
        elif not self.__position.exitActive():
            self.__position.exitMarket()

def main(codes, start_date, end_date, signal_period_unit = 5):
    '''
    signal_period_unit: 检测信号的时间间隔。与信号检测的周期保持一致。
    '''
    data = get_data(code, start_date, end_date)
    if data is None: return
    feed = dataFramefeed.Feed()
    feed.addBarsFromDataFrame(code, data)
    # Set Strategy
    brk = gen_broker(feed)
    myStrategy = MACDStrategy(feed, brk, data, codes, signal_period_unit)
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
        codes = ['002466', '601398', '600085', '000063', '002236', '000651', '002415', '600703', '300187', '002028']  # 股票池
        macd_cache = MacdCache(count = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM, stocks = codes)
        macd_cache.update_cache()
        for stock_code in macd_cache.bars.keys():
            # 获取最新一根bar检测到的背离、金叉、死叉
            divergences = g.macd_cache.divergences[stock_code] if stock_code in g.macd_cache.divergences.keys() else []
            last_bar = g.macd_cache.bars[stock_code].iloc[-1] if not g.macd_cache.bars[stock_code].empty else {}
            tm = last_bar.name
            if len(divergences) > 0:
                for divergence in divergences:
                    # DivergenceType.Bottom - 底背离，DivergenceType.Top - 顶背离
                    if divergence.divergence_type == DivergenceType.Bottom:
                        g.macd_signals.append(
                            MacdSignal(code=stock_code, period=g.macd_cache.period, tm=tm, name='BottomDivergence'))
                        log.info('【%s, %s】all divergences=%s' % (stock_code, current_tm, Divergence.to_json_list(divergences)))
                        break

            if 'gold' in last_bar.keys() and last_bar['gold']:
                g.macd_signals.append(MacdSignal(code=stock_code, period=g.macd_cache.period, tm=tm, name='Gold'))
                log.info('【%s, %s】Gold, last_bar=%s, ' % (stock_code, current_tm, last_bar.to_dict()))

            if 'death' in last_bar.keys() and last_bar['death']:
                g.macd_signals.append(MacdSignal(code=stock_code, period=g.macd_cache.period, tm=tm, name='Death'))
                log.info('【%s, %s】Death, last_bar=%s, ' % (stock_code, current_tm, last_bar.to_dict()))
    except Exception as e:
        traceback.print_exc()
