# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
from pyalgotrade import strategy
from pyalgotrade.broker import backtesting
from base.feed.livefeed import LiveFeed
from common import add_prifix, get_real_trading_stocks

class LiveTradingStrategy(strategy.BaseStrategy):
    def __init__(self, feed, brk, instruments):
        strategy.BaseStrategy.__init__(self, feed, brk)
        self.__instruments = instruments
        self.__priceDataSeries = feed[instruments[0]].getCloseDataSeries()
        self.__position = None

    def onEnterCanceled(self, position):
        self.__position = None

    def onEnterOK(self):
        pass

    def onExitOk(self, position):
        self.__position = None
        self.info("long close")

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def onBars(self, bars):
        bar = bars[self.__instruments[0]]
        print(bar.getDateTime(), bar.getAp(), bar.getAv())

def main():
    fpath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/trading.json"
    trading_info = get_real_trading_stocks(fpath)
    stocks  = [add_prifix(identifier) for identifier in trading_info['buy']]
    stocks.extend([add_prifix(identifier) for identifier in trading_info['sell']])
    liveFeed = LiveFeed(stocks, 3)
    brk = backtesting.Broker(1000, liveFeed)
    strat = LiveTradingStrategy(liveFeed, brk, stocks)
    strat.run()

if __name__ == "__main__": 
    main()
