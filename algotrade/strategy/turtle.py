# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import numpy as np
import pandas as pd
from cstock import CStock
from cindex import CIndex
from datetime import timedelta
from rstock import RIndexStock
from cstock_info import CStockInfo
from common import is_df_has_unexpected_data, delta_days
from algotrade.feed import dataFramefeed
from algotrade.indicator.macd import Macd, DivergenceType
from algotrade.strategy import gen_broker
from algotrade.plotter import plotter
from algotrade.technical.ma import macd
from algotrade.technical.atr import atr
from pyalgotrade import strategy
from pyalgotrade.technical import ma
from pyalgotrade.stratanalyzer import sharpe, drawdown
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, total_risk, instruments, feed, cash, fastEMA, slowEMA, signalEMA, maxLen, stockNum, duaration):
        self.total_risk = total_risk
        self.total_num = stockNum
        self.duaration = duaration
        strategy.BacktestingStrategy.__init__(self, feed, gen_broker(feed, cash * stockNum))
        self.instruments = instruments
        self.duaration_dict = dict()
        self.dealprice_dict = dict()
        self.high_dict = dict()
        self.add_count_dict = {}
        self.sma_dict = dict()
        self.macd_dict = dict()
        for instrument in instruments:
            self.sma_dict[instrument] = ma.SMA(feed[instrument].getPriceDataSeries(), duaration)
            self.macd_dict[instrument] = Macd(instrument, feed[instrument], fastEMA, slowEMA, signalEMA, maxLen)
        self.setUseAdjustedValues(False)

    def getInstruments(self):
        return self.instruments

    def getDif(self, instrument):
        return self.macd_dict[instrument].getDif()

    def getDea(self, instrument):
        return self.macd_dict[instrument].getDea()

    def getDealPrice(self, instrument):
        if instrument not in self.dealprice_dict: return None
        return self.dealprice_dict[instrument]

    def setDealPriceAndSubmitDateTime(self, order):
        if order.isFilled():
            instrument = order.getInstrument()
            if order.isBuy():
                self.dealprice_dict[instrument] = order.getAvgFillPrice()
                self.duaration_dict[instrument] = order.getSubmitDateTime()
                if instrument not in self.add_count_dict:
                    self.add_count_dict[instrument] = 0
                else:
                    self.add_count_dict[instrument] += 1
            elif order.isSell():
                if instrument in self.dealprice_dict: del self.dealprice_dict[instrument]
                if instrument in self.duaration_dict: del self.duaration_dict[instrument]
                if instrument in self.add_count_dict: del self.add_count_dict[instrument]

    def isInstrumentTimeout(self, instrument, bar):
        if instrument not in self.duaration_dict: return False
        if bar.getDateTime() > self.duaration_dict[instrument] + timedelta(days = self.duaration):
            return True
        return False

    def onOrderUpdated(self, order):
        self.setDealPriceAndSubmitDateTime(order)

    def getExpectdShares(self, price, atr):
        #成交量获取到的单位是股
        total_asserts = self.getBroker().getEquity()
        return 100 * int(total_asserts * self.total_risk / (atr * 100 * price))

    def getActualPostion(self):
        return self.getBroker().getPositions()

    def shouldAddPosition(self, instrument, bars):
        if instrument in bars.keys():
            atr = bars[instrument].getExtraColumns()['atr']
            dealPrice = self.getDealPrice(instrument)
            closePrice = bars[instrument].getPrice()
            if atr is None: return False
            if dealPrice is None: return False
            if closePrice > dealPrice + atr * 0.5:
                if instrument in self.add_count_dict and self.add_count_dict[instrument] >= MAX_ADD_POSITION: return False
                self.info("%s add position, price:%s, result:%s" % (instrument, closePrice, dealPrice + 0.5 * atr))
                return True
        return False

    def shouldStopLoss(self, instrument, bars):
        atr = bars[instrument].getExtraColumns()['atr']
        dealPrice = self.getDealPrice(instrument)
        closePrice = bars[instrument].getPrice()
        if atr is None: return False
        if dealPrice is None: return False
        if dealPrice <= closePrice - atr * STOP_LOSS_ATR:
            # 当前价格小于等于最高价回撤 STOP_LOSS_ATR 倍ATR，进行止损卖出
            self.info("%s stop loss, price:%s, result:%s" % (instrument, closePrice, dealPrice - atr * STOP_LOSS_ATR))
            return True
        return False

    def getAdjustSignal(self, bars):
        actualPostion = self.getActualPostion()
        adjustSignalList = list()
        for instrument in actualPostion:
            if instrument in self.instruments and instrument in bars.keys():
                if self.shouldStopLoss(instrument, bars):
                    newPosition = dict()
                    newPosition[instrument] = -1 * actualPostion[instrument]
                    adjustSignalList.append(newPosition)
                if self.shouldAddPosition(instrument, bars):
                    newPosition = dict()
                    atr = bars[instrument].getExtraColumns()['atr']
                    price = bars[instrument].getPrice()
                    newPosition[instrument] = self.getExpectdShares(price, atr)
                    adjustSignalList.append(newPosition)
        return adjustSignalList

    def getNewSignal(self, bars):
        position = dict()
        signalList = list()
        actualPostions = self.getActualPostion()
        for instrument in actualPostions:
            if instrument not in bars.keys(): continue
            if len(self.macd_dict[instrument].double_top_divergences) > 0:
                self.info("sell for double top divergence date:%s, instrument:%s" % (bars.getDateTime(), instrument))
                position[instrument] = -1 * actualPostions[instrument]
                signalList.append(position)

        for instrument in self.instruments:
            #self.info("buy for double bottom divergence date:%s, %s: double top divergence:%s" % (bars.getDateTime(), instrument, double_divergence.to_json()))
            if len(self.macd_dict[instrument].double_bottom_divergences) > 0 and instrument not in actualPostions and len(actualPostions) < self.total_num:
                self.info("buy for double bottom divergence date:%s, instrument:%s" % (bars.getDateTime(), instrument))
                atr = bars[instrument].getExtraColumns()['atr']
                price = bars[instrument].getPrice()
                position[instrument] = self.getExpectdShares(price, atr)
                signalList.append(position)
        return signalList 

    def getSignalDict(self, bars):
        newSignalList = self.getNewSignal(bars)
        adjustSignalList = self.getAdjustSignal(bars)
        adjustSignalList.extend(newSignalList)
        return adjustSignalList

    def onBars(self, bars):
        signalList = self.getSignalDict(bars)
        for info in signalList:
            for instrument, shares in info.items():
                self.marketOrder(instrument, shares, allOrNone = True)
            
def get_feed(all_df, codes, start_date, end_date, peried, duaration):
    feed = dataFramefeed.Feed()
    for code in codes:
        data = all_df.loc[all_df.code == code]
        data = data.sort_values(by=['date'], ascending = True)
        data = data.reset_index(drop = True)
        data = data.set_index('date')
        if is_df_has_unexpected_data(data): return None, None
        data.index = pd.to_datetime(data.index)
        data = data.dropna(how='any')
        data = atr(data, ndays = duaration)
        feed.addBarsFromDataFrame(code, data)
    return feed

MID   = 9
SHORT = 12
LONG  = 26
# 跟踪止损的ATR倍数，即买入后，从成交价回撤该倍数的ATR后止损
STOP_LOSS_ATR = 10
MAX_BAR_NUM = 250
MAX_ADD_POSITION = 4
def main(start_date, end_date, maxLen = MAX_BAR_NUM, peried = 'D'):
    all_df = get_stock_pool(start_date, end_date)
    codes = list(set(all_df.code.tolist()))
    #每只股票可投资的金额
    cash = 100000
    stockNum = 10
    totalRisk = 0.01
    duaration = 10 #调仓周期

    feed = get_feed(all_df, codes, start_date, end_date, peried, duaration)
    if feed is None: return False

    macdStrategy = MACDStrategy(totalRisk, codes, feed, cash, SHORT, LONG, MID, maxLen, stockNum, duaration)
    # Attach a drawdown analyzer to the strategy
    drawdownAnalyzer = drawdown.DrawDown()
    macdStrategy.attachAnalyzer(drawdownAnalyzer)
    # Attach a sharpe ratio analyzers to the strategy
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    macdStrategy.attachAnalyzer(sharpeRatioAnalyzer)
    # Attach the plotter to the strategy
    plt = plotter.StrategyPlotter(macdStrategy, False, True, True)
    plt.getOrCreateSubplot("sharpRatio").addDataSeries("sharpRatio", sharpeRatioAnalyzer.getReturns())
    # Run Strategy
    macdStrategy.run()
    macdStrategy.info("Get Max Downdown: %.2f" % drawdownAnalyzer.getMaxDrawDown())
    macdStrategy.info("Final portfolio value: %.2f" % macdStrategy.getResult())
    plt.plot()

def get_blacklist():
    black_list = ct.BLACK_DICT.keys()
    return black_list

def get_all_codelist():
    #返回不包含ST股票
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    df = stock_info_client.get(redis_host = '127.0.0.1')
    return df[~df.name.str.contains("ST")].code.tolist()

def get_stock_data(start_date, end_date):
    ris = RIndexStock(ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    return ris.get_k_data_in_range(start_date, end_date)

def get_stock_pool(start_date, end_date):
    '''
    更新股票池。该方法在收盘后调用。
    1. 全市场(不包含ST)股票作为基础股票池
    2. 剔除自制股票黑名单中的股票
    3. 剔除成交额小于1个亿的股票
    4. 剔除总市值中位数在100亿以下的股票
    5. 取25日跌幅前10%的股票作为最终的股票池
    '''
    #获取所有股票数据
    num_of_day = delta_days(start_date, end_date)
    all_df = get_stock_data(start_date, end_date)
    #黑名单股票
    black_list = get_blacklist()
    #出去黑名单中的股票
    all_code_list = get_all_codelist()
    all_code_list = list(set(all_code_list).difference(set(black_list)))
    all_df = all_df.loc[all_df.code.isin(all_code_list)]
    #获取开盘天数大于50%的股票的数量
    codes = list()
    amounts = list()
    outstandings = list()
    pchanges = list()
    for code, df in all_df.groupby('code'):
        tmps = df.loc[df.date == end_date, 'close']
        if tmps.empty: continue
        now_price = tmps.values[0] 
        df = df.sort_values(by=['date'], ascending = True)
        df = df.reset_index(drop = True)
        if len(df) > int(num_of_day * 0.5):
            codes.append(code)
            amounts.append(np.median(df.amount))
            close = np.median(df.close)
            open_ = df['open'][0] 
            close_ = df['close'][len(df) - 1]
            pchange = (close_ - open_) / open_
            pchanges.append(pchange)
            totals = np.median(df.totals)
            outstandings.append(now_price * totals)
    all_df = all_df.loc[all_df.code.isin(codes)]
    info = {'code': codes, 'amount': amounts, 'outstanding': outstandings, 'pchange': pchanges}
    stock_df = pd.DataFrame(info)
    stock_df = stock_df.reset_index(drop = True)
    #总市值大于100亿, 小于400亿的股票的列表
    stock_df = stock_df.sort_values(by=['outstanding'], ascending = False)
    biglist = stock_df.loc[(stock_df.outstanding > 1e10) & (stock_df.outstanding < 4e10)].code.tolist()
    all_df = all_df.loc[all_df.code.isin(biglist)]
    ##获取成交额中位数大于1亿的股票
    stock_df = stock_df.sort_values(by=['amount'], ascending = False)
    code_list = stock_df.loc[stock_df.amount > 1e8].code.tolist()
    all_df = all_df.loc[all_df.code.isin(code_list)]
    return all_df

if __name__ == '__main__':
    try:
        start_date = '2013-01-01'
        end_date = '2015-10-12'
        main(start_date, end_date)
    except Exception as e:
        print(e)
        traceback.print_exc()
