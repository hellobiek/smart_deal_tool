# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cstock import CStock
from futu import TrdEnv
from pyalgotrade import broker, strategy
from algotrade.feed import dataFramefeed
from algotrade.feed.futufeed import FutuFeed
from algotrade.broker.futu.futubroker import FutuBroker
from common import add_prifix, get_real_trading_stocks
class LiveTradingStrategy(strategy.BaseStrategy):
    def __init__(self, feed, brk, instruments):
        super(LiveTradingStrategy, self).__init__(feed, brk)
        self.__instruments = instruments
        self.__feed = feed
        self.__position = None

    def onEnterCanceled(self, position):
        self.__position = None

    def onEnterOK(self):
        pass

    def onExitOk(self, position):
        self.__position = None

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def onOrderUpdated(self, order):
        if order.isFilled():
            self.info("{} {} {} {} {} {}".format(order.getId(), order.getAction(), order.getInstrument(),
                              order.getQuantity(), order.getLimitPrice(), order.getSubmitDateTime()))

    def onBars(self, bars):
        instrument  = self.__instruments[0]
        bar         = bars[instrument]
        price       = bar.getClose()
        cash        = self.getBroker().getCash()
        shares      = self.getBroker().getPositions()
        self.info("code:{}, price:{}, cash:{}, shares:{}".format(instrument, price, cash, shares))
        quantity    = 100
        action      = broker.Order.Action.BUY
        order       = self.getBroker().createLimitOrder(action, instrument, price, quantity)
        self.getBroker().submitOrder(order)

def main():
    stocks = ['HK.00700']
    market = ct.HK_MARKET_SYMBOL
    deal_time = ct.MARKET_DEAL_TIME_DICT[market]
    timezone = ct.TIMEZONE_DICT[market]
    apath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/futu.json"
    dataFeed = FutuFeed(stocks, dealtime = deal_time, timezone = timezone)
    futuBroker = FutuBroker(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, 
                        market = market, timezone = timezone, dealtime = deal_time, unlock_path = apath)
    strat = LiveTradingStrategy(dataFeed, futuBroker, stocks)
    strat.run()

if __name__ == "__main__": 
    main()
