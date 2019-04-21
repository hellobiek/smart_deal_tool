# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import numpy as np
import pandas as pd
from cstock import CStock
from cindex import CIndex
from rstock import RIndexStock
from cstock_info import CStockInfo
from common import is_df_has_unexpected_data, get_day_nday_ago, delta_days
from algotrade.feed import dataFramefeed
from algotrade.indicator.macd import Macd, DivergenceType
from algotrade.strategy import gen_broker
from algotrade.plotter import plotter
from algotrade.technical.ma import macd
from pyalgotrade import strategy
from pyalgotrade.stratanalyzer import returns, sharpe
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, instrument, feed, brk, signal_period_unit, fastEMA, slowEMA, signalEMA, maxLen):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__position = None
        self.__instrument = instrument
        self.__checkPeriod = signal_period_unit
        self.macd = Macd(feed, fastEMA, slowEMA, signalEMA, maxLen, instrument)
        self.setUseAdjustedValues(False)

    def getInstrument(self):
        return self.__instrument

    def getDif(self):
        return self.macd.getDif()

    def getDea(self):
        return self.macd.getDea()

    def onEnterCanceled(self, position):
        self.info("enter onEnterCanceled")
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.info("enter onExitCanceled")
        self.__position.exitMarket()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("%s buy at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("%s sell at ￥%.2f" % (execInfo.getDateTime(), execInfo.getPrice()))
        self.__position = None

    def checkSignal(self, bars):
        if len(self.macd.divergences) == 0: return 0
        flag = 0
        self.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01")
        for divergence in self.macd.divergences:
            self.info("%s: divergence:%s, date:%s" % (self.getInstrument(), divergence.to_json(), bars.getDateTime()))
            if divergence.type == DivergenceType.Top:
                flag -= 1
            else:
                flag += 1
        self.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA02")
        return flag

    def onBars(self, bars):
        signal = self.checkSignal(bars)
        if 0 == signal: return
        if self.__position is None:
            if signal > 0:
                shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
                self.__position = self.enterLong(self.__instrument, shares, True)
        else:
            if signal < 0:
                self.__position.exitMarket()

def get_feed(all_df, code, start_date, end_date, peried):
    feed = dataFramefeed.Feed()
    #cindex_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    #cstock_obj = CStock(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    #data = cstock_obj.get_k_data_in_range(start_date, end_date)
    #data = data.set_index('date')
    data = all_df.loc[all_df.code == code]
    data = data.sort_values(by=['date'], ascending = True)
    data = data.reset_index(drop = True)
    data = data.set_index('date')
    if is_df_has_unexpected_data(data): return None, None
    data.index = pd.to_datetime(data.index)
    data = data.dropna(how='any')
    feed.addBarsFromDataFrame(code, data)
    return feed

MID   = 9
SHORT = 12
LONG  = 26
SIGNAL_PERIOD_UNIT = 29 #检测信号的时间间隔。与信号检测的周期保持一致。
DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM = 250
def main(start_date, end_date, maxLen = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM, peried = 'D'):
    '''
    count: 采用过去count个bar内极值的最大值作为参考。
    '''
    all_df = get_stock_pool(start_date, end_date)
    codes  = list(set(all_df.code.tolist()))
    strategys = dict()
    for code in codes:
        feed = get_feed(all_df, code, start_date, end_date, peried)
        if feed is None: return False
        brk = gen_broker(feed)
        strategys[code] = MACDStrategy(code, feed, brk, SIGNAL_PERIOD_UNIT, SHORT, LONG, MID, maxLen)
        # Attach a returns analyzers to the strategy
        returnsAnalyzer = returns.Returns()
        strategys[code].attachAnalyzer(returnsAnalyzer)
        # Attach a sharpe ratio analyzers to the strategy
        sharpeRatioAnalyzer = sharpe.SharpeRatio()
        strategys[code].attachAnalyzer(sharpeRatioAnalyzer)
        # Attach the plotter to the strategy
        plt = plotter.StrategyPlotter(strategys[code], True, True, True)
        plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())
        plt.getOrCreateSubplot("Macd").addDataSeries("dif", strategys[code].getDif())
        plt.getOrCreateSubplot("Macd").addDataSeries("dea", strategys[code].getDea())
        # Run Strategy
        strategys[code].run()
        strategys[code].info("Final portfolio value: $%.2f" % strategys[code].getResult())
        plt.plot()

def get_blacklist():
    black_list = ct.BLACK_DICT.keys()
    return black_list

def get_all_codelist():
    #返回不包含ST股票
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    df = stock_info_client.get(redis_host = '127.0.0.1')
    return df[~df.name.str.contains("ST")].code.tolist()

def get_stock_data(start_date, end_date, num):
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
    num = delta_days(start_date, end_date)
    all_df = get_stock_data(start_date, end_date, num)
    #黑名单股票
    black_list = get_blacklist()
    #所有单股票
    all_code_list = get_all_codelist()
    all_code_list = list(set(all_code_list).difference(set(black_list)))
    all_df = all_df.loc[all_df.code.isin(all_code_list)]
    #获取开盘天数大于30%的股票的数量
    codes = list()
    amounts = list()
    outstandings = list()
    pchanges = list()
    for code, df in all_df.groupby('code'):
        df = df.sort_values(by=['date'], ascending = True)
        df = df.reset_index(drop = True)
        if len(df) > int(num * 0.5):
            codes.append(code)
            amounts.append(np.median(df.amount))
            close = np.median(df.close)
            open_ = df['open'][0] 
            close_ = df['close'][len(df) - 1]
            pchange = (close_ - open_) / open_
            pchanges.append(pchange)
            totals = np.median(df.totals)
            outstandings.append(close * totals)
    all_df = all_df.loc[all_df.code.isin(codes)]
    info = {'code': codes, 'amount': amounts, 'outstanding': outstandings, 'pchange': pchanges}
    stock_df = pd.DataFrame(info)
    stock_df = stock_df.reset_index(drop = True)
    #总市值大于100亿的股票的列表
    stock_df = stock_df.sort_values(by=['outstanding'], ascending = False)
    biglist = stock_df.loc[stock_df.outstanding > 1e10].code.tolist()
    all_df = all_df.loc[all_df.code.isin(biglist)]
    #获取成交额大于1个亿的股票
    stock_df = stock_df.sort_values(by=['amount'], ascending = False)
    code_list = stock_df.loc[stock_df.amount > 1e8].code.tolist()
    all_df = all_df.loc[all_df.code.isin(code_list)]
    #取25日跌幅前10%的股票
    stock_df = stock_df.sort_values(by=['pchange'], ascending = True)
    code_list = stock_df.head(int(len(stock_df) * 0.1)).code.tolist()
    all_df = all_df.loc[all_df.code.isin(code_list)]
    return all_df

def cross(short_mean,long_mean):
    '''
    判断短时均线和长时均线的关系。
    Args:
        short_mean 短时均线，长度不应小于3
        long_mean  长时均线，长度不应小于3。
    Returns:
         1 短时均线上穿长时均线
         0 短时均线和长时均线未发生交叉
        -1 短时均线下穿长时均线
    '''
    delta = short_mean[-3:] - long_mean[-3:]
    if (delta[-1] > 0) and ((delta[-2] < 0) or ((delta[-2] == 0) and (delta[-3] < 0))):
        return 1
    elif (delta[-1] < 0) and ((delta[-2] > 0) or ((delta[-2] == 0) and (delta[-3] > 0))):
        return -1
    return 0

if __name__ == '__main__':
    try:
        start_date = '2017-01-01'
        end_date   = '2019-04-19'
        main(start_date, end_date)
    except Exception as e:
        print(e)
