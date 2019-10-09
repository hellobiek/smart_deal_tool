# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
from futu import TrdEnv
from pyalgotrade import strategy
from pyalgotrade.broker import Order
from algotrade.plotter import plotter
from base.cdate import datetime_to_str
from algotrade.strategy import gen_broker
from algotrade.feed.localfeed import LocalFeed
from pyalgotrade.stratanalyzer import returns, sharpe
from algotrade.broker.futu.futubroker import FutuBroker
from algotrade.model.silverlining import SilverLiningModel
class SilverLiningStrategy(strategy.BaseStrategy):
    def __init__(self, model, instruments, feed, brk):
        super(SilverLiningStrategy, self).__init__(feed, brk)
        self.model = model
        self.instruments = instruments
        self.setUseEventDateTimeInLogs(True)

    def getExpectdShares(self, price, cash):
        #成交量获取到的单位是股
        return 100 * int(cash / (100 * price))

    def getCash(self):
        return self.getBroker().getCash()

    def getActualPostion(self):
        return self.getBroker().getPositions()

    def getSignalDict(self, bars):
        position = dict()
        actualPostion = self.getActualPostion()
        for code in self.instruments:
            bar = bars.getBar(code)
            if bar is None: continue
            row = bar.getExtraColumns()
            ratio = row['ratio']
            if ratio < 3:
                if actualPostion is None or not actualPostion.code.str.endswith(code).any():
                    price = bar.getPrice() * 1.02
                    cash = self.getCash()
                    position[code] = dict()
                    position[code]['price'] = price
                    position[code]['quantity'] = self.getExpectdShares(price, cash)
                    self.info("will buy: {} {} at {}".format(code, position[code]['quantity'], position[code]['price']))

        if actualPostion is not None:
            for _, item in actualPostion.iterrows():
                code = item['code'].split('.')[1]
                cost_price = item['cost_price']
                bar = bars.getBar(code)
                if bar is None: continue
                row = bar.getExtraColumns()
                ratio = row['ratio']
                price = bar.getPrice()
                if ratio > 30:
                    position[code] = dict()
                    position[code]['price'] = item['nominal_price'] * 0.98
                    position[code]['quantity'] = -1 * item['qty']
                    self.info("will sell: {} at {} for {} bull ratio more than 30".format(code, position[code], position[code]['price']))
                    continue
        return position

    def onOrderUpdated(self, order):
        if order.isFilled():
            msg = "buy" if order.isBuy() else "sell"
            instrument = order.getInstrument()
            price = order.getAvgFillPrice()
            quantity = order.getQuantity()
            self.debug("{} {} at {} for {} succeed".format(msg, instrument, price, quantity))

    def onBars(self, bars):
        signalList = self.getSignalDict(bars)
        self.debug("get signals: {}".format(signalList))
        for instrument, info in signalList.items():
            price = info['price']
            quantity = info['quantity']
            action = Order.Action.BUY if quantity > 0 else Order.Action.SELL
            order = self.getBroker().createLimitOrder(action, instrument, price, abs(quantity))
            self.getBroker().submitOrder(order, self.model.code)

def main(model, feed, brk, codes):
    mStrategy = SilverLiningStrategy(model, codes, feed, brk)
    # Attach a returns analyzers to the strategy
    returnsAnalyzer = returns.Returns()
    mStrategy.attachAnalyzer(returnsAnalyzer)
    # Attach a sharpe ratio analyzers to the strategy
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    mStrategy.attachAnalyzer(sharpeRatioAnalyzer)
    # Attach the plotter to the strategy
    plt = plotter.StrategyPlotter(mStrategy, False, True, True)
    plt.getOrCreateSubplot("returns").addDataSeries("returns", returnsAnalyzer.getReturns())
    plt.getOrCreateSubplot("sharpRatio").addDataSeries("sharpRatio", sharpeRatioAnalyzer.getReturns())
    # Run Strategy
    mStrategy.run()
    mStrategy.info("Final portfolio value: $%.2f" % mStrategy.getResult())
    plt.plot()

def paper_trading():
    cash = 1000000
    start_date = '2014-01-01'
    end_date   = '2019-10-09'
    dbinfo = ct.OUT_DB_INFO
    market = ct.CN_MARKET_SYMBOL
    redis_host = '127.0.0.1'
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    model = SilverLiningModel(dbinfo = dbinfo, redis_host = redis_host, cal_file_path = cal_file_path, should_create_mysqldb = True)
    feed, code_list = model.generate_feed(start_date, end_date)
    broker = gen_broker(feed, cash = cash, market = market)
    main(model, feed, broker, code_list)

def real_trading():
    market = ct.CN_MARKET_SYMBOL
    deal_time = ct.MARKET_DEAL_TIME_DICT[market]
    timezone = ct.TIMEZONE_DICT[market]
    apath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/follow_trend.json"
    kpath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/key.pri"
    dbinfo = ct.OUT_DB_INFO
    redis_host = '127.0.0.1'
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    model = SilverLiningModel(dbinfo = dbinfo, redis_host = redis_host, cal_file_path = cal_file_path, should_create_mysqldb = True)
    code_list = list()
    broker = FutuBroker(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, #SIMULATE
                        market = market, timezone = timezone, dealtime = deal_time, unlock_path = apath, key_path = kpath)
    feed = LocalFeed(model, broker, code_list, dealtime = deal_time, timezone = timezone, frequency = 24 * 60 * 60)
    main(model, feed, broker, code_list)

if __name__ == '__main__':
    try:
        #real_trading()
        paper_trading()
    except Exception as e:
        traceback.print_exc()
