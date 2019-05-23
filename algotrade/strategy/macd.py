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
from common import is_df_has_unexpected_data, get_day_nday_ago, delta_days
from algotrade.feed import dataFramefeed
from algotrade.indicator.macd import Macd, DivergenceType
from algotrade.strategy import gen_broker
from algotrade.plotter import plotter
from algotrade.technical.ma import macd
from algotrade.technical.atr import atr
from algotrade.technical.arf import arf
from pyalgotrade import strategy
from pyalgotrade.technical import ma
from pyalgotrade.stratanalyzer import returns, sharpe
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, total_risk, instruments, feed, cash, fastEMA, slowEMA, signalEMA, maxLen, stockNum, duaration):
        self.__total_risk = total_risk
        self.__total_num = stockNum
        self.__duaration = duaration
        brk = gen_broker(feed, cash * stockNum)
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__instruments = instruments
        self.__high_dict = dict()
        self.__sma_dict = dict()
        self.__macd_dict = dict()
        self.__duaration_dict = dict()
        for instrument in instruments:
            self.__sma_dict[instrument] = ma.SMA(feed[instrument].getPriceDataSeries(), duaration)
            self.__macd_dict[instrument] = Macd(instrument, feed[instrument], fastEMA, slowEMA, signalEMA, maxLen)
        self.setUseAdjustedValues(False)

    def getInstruments(self):
        return self.__instruments

    def getDif(self, instrument):
        return self.__macd_dict[instrument].getDif()

    def getDea(self, instrument):
        return self.__macd_dict[instrument].getDea()

    def getHighestPrice(self, instrument):
        if instrument not in self.__high_dict: return None
        return self.__high_dict[instrument]

    def updateHighestPrice(self, bars):
        for instrument in self.getActualPostion():
            highPrice = self.getHighestPrice(instrument)
            if highPrice is not None and instrument in bars.keys():
                highPrice = max(bars[instrument].getHigh(), highPrice)
                self.__high_dict[instrument] = highPrice

    def setHighestPriceAndSubmitDateTime(self, order):
        if order.isFilled():
            instrument = order.getInstrument()
            if instrument not in self.__high_dict:
                self.__high_dict[instrument] = order.getAvgFillPrice()
                self.__duaration_dict[instrument] = order.getSubmitDateTime()

    def isInstrumentTimeout(self, instrument, bar):
        if instrument not in self.__duaration_dict: return False
        if bar.getDateTime() > self.__duaration_dict[instrument] + timedelta(days = self.__duaration):
            return True
        return False

    def onOrderUpdated(self, order):
        self.setHighestPriceAndSubmitDateTime(order)

    def getExpectdShares(self, risk_adjust_factor, sigma, position_sigma = 5):
        #成交量获取到的单位是手，所以这里转换为手（1手=100股）
        total_asserts = self.getBroker().getEquity()
        return int(total_asserts * self.__total_risk * risk_adjust_factor / ((position_sigma * sigma) * 100)) * 100

    def getActualPostion(self):
        return self.getBroker().getPositions()

    def shouldStopAndLoss(self, instrument, bars):
        if instrument in bars.keys():
            atr = bars[instrument].getExtraColumns()['atr']
            highPrice = self.getHighestPrice(instrument)
            closePrice = bars[instrument].getPrice()
            if atr is None: return 0
            if highPrice is None: return 0
            if closePrice <= highPrice - atr * TRAILING_STOP_LOSS_ATR:
                # 当前价格小于等于最高价回撤 TRAILING_STOP_LOSS_ATR 倍ATR，进行止盈止损卖出
                self.info("%s should sell now, price:%s, result:%s" % (instrument, closePrice, highPrice - atr * TRAILING_STOP_LOSS_ATR))
                return 1
            if self.isInstrumentTimeout(instrument, bars[instrument]):
                self.info("%s should sell now for timeout" % instrument)
                return 1
        return 0

    def getAdjustSignal(self, bars):
        actualPostion = self.getActualPostion()
        adjustSignalList = list()
        for instrument in actualPostion:
            if instrument in self.__instruments and instrument in bars.keys():
                if self.shouldStopAndLoss(instrument, bars):
                    newPosition = dict()
                    newPosition[instrument] = -1 * actualPostion[instrument]
                    adjustSignalList.append(newPosition)
                else:
                    expectPosition = dict()
                    sigma = bars[instrument].getExtraColumns()['sigma']
                    risk_adjust_factor = bars[instrument].getExtraColumns()['arf']
                    expectPosition[instrument] = self.getExpectdShares(risk_adjust_factor, sigma)
                    deltaShares = expectPosition[instrument] - actualPostion[instrument]
                    if deltaShares != 0:
                        deltaPosition = dict()
                        deltaPosition[instrument] = expectPosition[instrument] - actualPostion[instrument]
                        adjustSignalList.append(deltaPosition)
            else:#todo
                pass
        return adjustSignalList

    def getNewSignal(self, bars):
        signalList = list()
        for instrument in self.__instruments:
            position = dict()
            actualPostions = self.getActualPostion()
            for instrument in actualPostions:
                if bars[instrument].getClose() < self.__sma_dict[instrument]:
                    #self.info("sell for double top divergence date:%s, %s: double top divergence:%s" % (bars.getDateTime(), instrument, double_divergence.to_json()))
                    self.info("sell for double top divergence date:%s, instrument:%s" % (bars.getDateTime(), instrument))
                    position[instrument] = -1 * actualPostions[instrument]
                    signalList.append(position)
            for double_divergence in self.__macd_dict[instrument].double_bottom_divergences:
                #self.info("buy for double bottom divergence date:%s, %s: double top divergence:%s" % (bars.getDateTime(), instrument, double_divergence.to_json()))
                if instrument not in self.getActualPostion() and len(actualPostions) < self.__total_num:
                    self.info("buy for double bottom divergence date:%s, instrument:%s" % (bars.getDateTime(), instrument))
                    sigma = bars[instrument].getExtraColumns()['sigma']
                    risk_adjust_factor = bars[instrument].getExtraColumns()['arf']
                    position[instrument] = self.getExpectdShares(risk_adjust_factor, sigma)
                    signalList.append(position)
        return signalList 

    def getSignalDict(self, bars):
        newSignalList = self.getNewSignal(bars)
        adjustSignalList = self.getAdjustSignal(bars)
        adjustSignalList.extend(newSignalList)
        return adjustSignalList

    def onBars(self, bars):
        self.updateHighestPrice(bars)
        signalList = self.getSignalDict(bars)
        for info in signalList:
            for instrument, shares in info.items():
                self.marketOrder(instrument, shares, allOrNone = True)
            
def get_feed(all_df, codes, start_date, end_date, peried):
    feed = dataFramefeed.Feed()
    for code in codes:
        data = all_df.loc[all_df.code == code]
        data = data.sort_values(by=['date'], ascending = True)
        data = data.reset_index(drop = True)
        data = data.set_index('date')
        if is_df_has_unexpected_data(data): return None, None
        data.index = pd.to_datetime(data.index)
        data = data.dropna(how='any')
        data = atr(data)
        data = arf(data)
        feed.addBarsFromDataFrame(code, data)
    return feed

MID   = 9
SHORT = 12
LONG  = 26
# 跟踪止损的ATR倍数，即买入后，从最高价回撤该倍数的ATR后止损
TRAILING_STOP_LOSS_ATR = 7
DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM = 250
def main(start_date, end_date, maxLen = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM, peried = 'D'):
    all_df = get_stock_pool(start_date, end_date)
    codes = list(set(all_df.code.tolist()))
    feed = get_feed(all_df, codes, start_date, end_date, peried)
    if feed is None: return False
    #每只股票可投资的金额
    cash = 100000
    stockNum = 10
    totalRisk = 0.1
    duaration = 10 #调仓周期
    macdStrategy = MACDStrategy(totalRisk, codes, feed, cash, SHORT, LONG, MID, maxLen, stockNum, duaration)
    # Attach a returns analyzers to the strategy
    returnsAnalyzer = returns.Returns()
    macdStrategy.attachAnalyzer(returnsAnalyzer)
    # Attach a sharpe ratio analyzers to the strategy
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    macdStrategy.attachAnalyzer(sharpeRatioAnalyzer)
    # Attach the plotter to the strategy
    plt = plotter.StrategyPlotter(macdStrategy, False, True, True)
    plt.getOrCreateSubplot("returns").addDataSeries("returns", returnsAnalyzer.getReturns())
    plt.getOrCreateSubplot("sharpRatio").addDataSeries("sharpRatio", sharpeRatioAnalyzer.getReturns())
    #plt.getOrCreateSubplot("Macd").addDataSeries("dif", strategys[code].getDif())
    #plt.getOrCreateSubplot("Macd").addDataSeries("dea", strategys[code].getDea())
    # Run Strategy
    macdStrategy.run()
    macdStrategy.info("Final portfolio value: $%.2f" % macdStrategy.getResult())
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
        start_date = '2018-01-01'
        end_date   = '2019-05-17'
        main(start_date, end_date)
    except Exception as e:
        print(e)
        traceback.print_exc()
