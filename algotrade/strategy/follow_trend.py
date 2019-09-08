# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import numpy as np
import pandas as pd
from futu import TrdEnv
from pyalgotrade import strategy
from pyalgotrade.broker import Order
from algotrade.plotter import plotter
from base.cdate import datetime_to_str
from algotrade.model.qmodel import QModel
from algotrade.strategy import gen_broker
from algotrade.feed.localfeed import LocalFeed
from pyalgotrade.stratanalyzer import returns, sharpe
from algotrade.broker.futu.futubroker import FutuBroker
class FollowTrendStrategy(strategy.BaseStrategy):
    def __init__(self, model, instruments, feed, brk, stockNum, duaration):
        super(FollowTrendStrategy, self).__init__(feed, brk)
        self.model = model
        self.tradingDays = 0
        ###############################
        #进程重启后，positions需要更新#
        ###############################
        self.positions = dict()
        self.totalNum = stockNum
        self.duaration = duaration
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
        acutalNum = len(actualPostion)
        for code in self.instruments:
            bar = bars.getBar(code)
            if bar is None: continue
            row = bar.getExtraColumns()
            k, d = row['k'], row['d']
            if k is None or d is None: continue
            if acutalNum >= self.totalNum:
                self.info("can not buy for actualNum {} >= totalNum {}".format(acutalNum, self.totalNum))
                continue
            if k < 15 and d < 15 and code not in actualPostion and row['ppercent'] > row['npercent']:
                price = bar.getPrice() * 1.02
                cash = self.getCash()
                cash = cash / (self.totalNum - acutalNum)
                position[code] = dict()
                position[code]['price'] = price
                position[code]['quantity'] = self.getExpectdShares(price, cash)
                self.info("buy: {} {} at {}".format(code, position[code]['quantity'], position[code]['price']))

        for code in actualPostion:
            bar = bars.getBar(code)
            if bar is None:continue
            price = bar.getPrice()
            row = bar.getExtraColumns()
            k, d = row['k'], row['d']
            if k is None or d is None: continue
            if row['ppercent'] <= row['npercent']:
                position[code] = dict()
                position[code]['price'] = price * 0.98
                position[code]['quantity'] = -1 * actualPostion[code]
                self.info("sell: {} {} for ppercent {} <= npercent {} at price: {}".format(code, 
                                        position[code], row['ppercent'], row['npercent'], price))
                continue

            if self.positions[code]['price'] > price * 1.2:
                assert(self.positions[code]['quantity'] == actualPostion[code])
                position[code] = dict()
                position[code]['price'] = price * 0.98
                position[code] = -1 * actualPostion[code]
                self.info("sell: {} {} for hold_price {} > 1.2 * cur_price {}".format(code,
                                    position[code], self.positions[code]['price'], price))
                continue
                
            if k > 80 and d > 80:
                position[code] = dict()
                position[code]['price'] = price * 0.98
                position[code]['quantity'] = -1 * actualPostion[code]
                self.info("sell: {} {} for kdj > 80 at price: {}".format(code, position[code]['quantity'], position[code]['price']))
                continue
        return position

    def onOrderUpdated(self, order):
        if order.isFilled():
            instrument = order.getInstrument()
            price = order.getAvgFillPrice()
            quantity = order.getQuantity()
            self.model.record(order)
            if order.isBuy():
                if instrument in self.positions:
                    self.positions[instrument]['price'] = (self.positions[instrument]['quantity'] * self.positions[instrument]['price'] 
                                                            + price * quantity) / (quantity + self.positions[instrument]['quantity'])
                    self.positions[instrument]['quantity'] += quantity
                else:
                    self.positions[instrument] = dict()
                    self.positions[instrument]['price'] = price
                    self.positions[instrument]['quantity'] = quantity
            elif order.isSell():
                if self.positions[instrument]['quantity'] == quantity:
                    del self.positions[instrument]
                else:
                    self.positions[instrument]['price'] = (self.positions[instrument]['quantity'] * self.positions[instrument]['price'] 
                                                           - price * quantity) / (self.positions[instrument]['quantity'] - quantity)
                    self.positions[instrument]['quantity'] = self.positions[instrument]['quantity'] - quantity

    def updateInstruments(self, bars):
        self.tradingDays += 1
        if self.tradingDays % self.duaration == 0:
            today = datetime_to_str(bars.getDateTime(), dformat = "%Y-%m-%d")
            data = self.model.get_stock_pool(today)
            if data.empty: return list()
            val = data.loc[data.date == today]['code']
            if type(val) == str:
                self.instruments = list(val)
            else:
                self.instruments = val.tolist()

    def onBars(self, bars):
        self.updateInstruments(bars)
        signalList = self.getSignalDict(bars)
        for instrument, info in signalList.items():
            price = info['price']
            quantity = info['quantity']
            action = Order.Action.BUY if quantity > 0 else Order.Action.SELL
            order = self.getBroker().createLimitOrder(action, instrument, price, abs(quantity))
            self.getBroker().submitOrder(order)

def main(model, feed, brk, codes, stock_num, duaration):
    mStrategy = FollowTrendStrategy(model, codes, feed, brk, stock_num, duaration)
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

def paper_trading(cash = 125000, stock_num = 4, duaration = 10):
    start_date = '2017-06-01'
    end_date   = '2019-08-23'
    dbinfo = ct.OUT_DB_INFO
    redis_host = '127.0.0.1'
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    model = QModel('follow_trend', valuation_path, bonus_path, stocks_dir,
                   base_stock_path, report_dir, report_publish_dir, pledge_file_dir,
                   rvaluation_dir, cal_file_path, dbinfo = dbinfo, 
                   redis_host = redis_host, should_create_mysqldb = True)
    feed, code_list = model.generate_feed(start_date, end_date)
    broker = gen_broker(feed, cash * stock_num)
    main(model, feed, broker, code_list, stock_num, duaration)

def real_trading(stock_num = 4, duaration = 10):
    market = ct.CN_MARKET_SYMBOL
    deal_time = ct.MARKET_DEAL_TIME_DICT[market]
    timezone = ct.TIMEZONE_DICT[market]
    apath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/futu.json"
    dbinfo = ct.OUT_DB_INFO
    redis_host = '127.0.0.1'
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    model = QModel('follow_trend', valuation_path, bonus_path, stocks_dir,
                   base_stock_path, report_dir, report_publish_dir, pledge_file_dir,
                   rvaluation_dir, cal_file_path, dbinfo = dbinfo,
                   redis_host = redis_host, should_create_mysqldb = True)
    code_list = list()
    broker = FutuBroker(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, #SIMULATE
                        market = market, timezone = timezone, dealtime = deal_time, unlock_path = apath)
    feed = LocalFeed(model, broker, code_list, dealtime = deal_time, timezone = timezone, frequency = 24 * 60 * 60)
    main(model, feed, broker, code_list, stock_num, duaration)

if __name__ == '__main__':
    try:
        real_trading()
    except Exception as e:
        print(e)
        traceback.print_exc()
