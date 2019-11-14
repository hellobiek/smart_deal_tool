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
from algotrade.strategy import gen_broker
from algotrade.feed.localfeed import LocalFeed
from pyalgotrade.stratanalyzer import returns, sharpe
from algotrade.broker.futu.futubroker import FutuBroker
from algotrade.model.follow_trend import FollowTrendModel
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
class FollowTrendStrategy(strategy.BaseStrategy):
    def __init__(self, model, instruments, feed, brk, stockNum, duaration):
        super(FollowTrendStrategy, self).__init__(feed, brk)
        self.model = model
        self.tradingDays = 0
        ###############################
        #进程重启后，positions需要更新#
        ###############################
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
        self.debug("actualPostion:{}".format(actualPostion))
        acutalNum = 0 if actualPostion is None else len(actualPostion)
        for code in self.instruments:
            bar = bars.getBar(code)
            if bar is None: continue
            row = bar.getExtraColumns()
            if acutalNum >= self.totalNum:
                self.debug("can not buy for actualNum {} >= totalNum {}".format(acutalNum, self.totalNum))
                continue
            
            should_continue = False
            for key in self.model.feature_list:
                if row[key] is None:
                    should_continue = True
                    break
            if should_continue: continue
            decider = self.model.load(code)
            vector = np.array([[row[key] for key in self.model.feature_list]]).reshape(1, -1)
            result = decider.predict(vector)[0]
            if result > 0:
                if actualPostion is None or not actualPostion.code.str.endswith(code).any():
                    price = bar.getPrice() * 1.02
                    cash = self.getCash()
                    cash = cash / (self.totalNum - acutalNum)
                    position[code] = dict()
                    position[code]['price'] = price
                    position[code]['quantity'] = self.getExpectdShares(price, cash)
                    self.info("will buy: {} {} at {}".format(code, position[code]['quantity'], position[code]['price']))

        if actualPostion is not None:
            for _, item in actualPostion.iterrows():
                code = item['code'].split('.')[1]
                cost_price = item['cost_price']
                bar = bars.getBar(code)
                if bar is None:continue
                row = bar.getExtraColumns()
                price = bar.getPrice()
                if (item['nominal_price'] - item['cost_price']) / item['cost_price'] <= -0.06:
                    position[code] = dict()
                    position[code]['price'] = item['nominal_price'] * 0.98
                    position[code]['quantity'] = -1 * item['qty']
                    self.info("will sell: {} at {} for {} hlzh less than 0".format(code, position[code], position[code]['price'], item['qty']))

                if (item['nominal_price'] - item['cost_price']) / item['cost_price'] >= 0.09:
                    position[code] = dict()
                    position[code]['price'] = item['nominal_price'] * 0.98
                    position[code]['quantity'] = -1 * item['qty']
                    self.info("will sell: {} at {} for {} profit more than 10%".format(code, position[code], position[code]['price'], item['qty']))
        return position

    def onOrderUpdated(self, order):
        if order.isFilled():
            msg = "buy" if order.isBuy() else "sell"
            instrument = order.getInstrument()
            price = order.getAvgFillPrice()
            quantity = order.getQuantity()
            self.debug("{} {} at {} for {} succeed".format(msg, instrument, price, quantity))

    def updateInstruments(self, bars):
        self.instruments = list()
        mdate = datetime_to_str(bars.getDateTime(), dformat = "%Y-%m-%d")
        data = self.model.get_stock_pool(mdate)
        data = data.loc[data.days >= 21]
        data = data.reset_index(drop = True)
        if data.empty: return self.instruments
        code_list = data.code.tolist()
        self.instruments = code_list

    def onBars(self, bars):
        self.updateInstruments(bars)
        signalList = self.getSignalDict(bars)
        self.debug("get signals: {}".format(signalList))
        for instrument, info in signalList.items():
            price = info['price']
            quantity = info['quantity']
            if quantity == 0: continue
            action = Order.Action.BUY if quantity > 0 else Order.Action.SELL
            order = self.getBroker().createLimitOrder(action, instrument, price, abs(quantity))
            self.getBroker().submitOrder(order, self.model.code)

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

def paper_trading(cash = 100000, stock_num = 10, duaration = 10):
    start_date = '2018-01-01'
    end_date   = '2019-11-04'
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
    model_dir = "/Volumes/data/quant/stock/data/models"
    model = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, 
                    report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, model_dir,
                    dbinfo = dbinfo, redis_host = redis_host, should_create_mysqldb = True)
    feed, code_list = model.generate_feed(start_date, end_date)
    broker = gen_broker(feed, cash * stock_num)
    main(model, feed, broker, code_list, stock_num, duaration)

def real_trading(stock_num = 10, duaration = 10):
    market = ct.CN_MARKET_SYMBOL
    deal_time = ct.MARKET_DEAL_TIME_DICT[market]
    timezone = ct.TIMEZONE_DICT[market]
    apath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/follow_trend.json"
    kpath = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/key.pri"
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
    model_dir = "/Volumes/data/quant/stock/data/models"
    model = FollowTrendModel(valuation_path, bonus_path, stocks_dir,
                   base_stock_path, report_dir, report_publish_dir, pledge_file_dir,
                   rvaluation_dir, cal_file_path, model_dir, dbinfo = dbinfo,
                   redis_host = redis_host, should_create_mysqldb = True)
    code_list = list()
    broker = FutuBroker(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, #SIMULATE
                        market = market, timezone = timezone, dealtime = deal_time, unlock_path = apath, key_path = kpath)
    feed = LocalFeed(model, broker, code_list, dealtime = deal_time, timezone = timezone, frequency = 24 * 60 * 60)
    main(model, feed, broker, code_list, stock_num, duaration)

if __name__ == '__main__':
    try:
        #real_trading()
        paper_trading()
    except Exception as e:
        traceback.print_exc()
