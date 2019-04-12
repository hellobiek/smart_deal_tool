# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import traceback
import const as ct
import pandas as pd
from cstock import CStock
from cindex import CIndex
from rstock import RIndexStock
from cstock_info import CStockInfo
from common import is_df_has_unexpected_data, get_day_nday_ago
from algotrade.feed import dataFramefeed
from algotrade.technical.ma import macd
from algotrade.indicator.macd import Macd
from algotrade.strategy import gen_broker
from pyalgotrade import strategy, plotter
from pyalgotrade.stratanalyzer import returns, sharpe
class MACDStrategy(strategy.BacktestingStrategy):
    def __init__(self, instruments, feeds, brk, signal_period_unit, fastEMA, slowEMA, signalEMA, maxLen):
        strategy.BacktestingStrategy.__init__(self, feed, brk)
        self.__position = None
        self.__instrument = instruments
        self.__signal_period_unit = signal_period_unit
        self.__macd = Macd(feeds, fastEMA, slowEMA, signalEMA, maxLen, instruments)
        self.setUseAdjustedValues(False)

    def getDif(self):
        return self.__macd.getDif()

    def getDea(self):
        return self.__macd.getDea()

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
        pass

def get_feed(codes, start_date, end_date, peried):
    feed = dataFramefeed.Feed()
    for code in codes:
        #cindex_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        cstock_obj = CStock(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
        data = cstock_obj.get_k_data_in_range(start_date, end_date)
        data = data.set_index('date')
        if is_df_has_unexpected_data(data): return None, None
        data.index = pd.to_datetime(data.index)
        data = data.dropna(how='any')
        feed.addBarsFromDataFrame(code, data)
    return feed

SHORT = 12
LONG  = 26
MID   = 9
DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM = 250
def main(start_date, end_date, maxLen = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM, signal_period_unit = 5, peried = 'D'):
    '''
    count: 采用过去count个bar内极值的最大值作为参考。
    signal_period_unit: 检测信号的时间间隔。与信号检测的周期保持一致。
    '''
    codes = get_stock_pool()
    feeds = get_feed(codes, start_date, end_date, peried)
    if feeds is None: return
    brk = gen_broker(feeds)
    myStrategy = MACDStrategy(feeds, codes, brk, signal_period_unit, SHORT, LONG, MID, maxLen)
    # Attach a returns analyzers to the strategy
    returnsAnalyzer = returns.Returns()
    myStrategy.attachAnalyzer(returnsAnalyzer)
    # Attach a sharpe ratio analyzers to the strategy
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    myStrategy.attachAnalyzer(sharpeRatioAnalyzer)
    # Attach the plotter to the strategy
    plt = plotter.StrategyPlotter(myStrategy, True, True, True)
    plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())
    plt.getOrCreateSubplot("Macd").addDataSeries("dif", myStrategy.getDif())
    plt.getOrCreateSubplot("Macd").addDataSeries("dea", myStrategy.getDea())
    # Run Strategy
    myStrategy.run()
    myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())
    plt.plot()

def get_blacklist():
    black_list = ct.BLACK_DICT.keys()
    return black_list

def get_all_codelist():
    #返回不包含ST股票
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    df = stock_info_client.get(redis_host = '127.0.0.1')
    return df[~df.name.str.contains("ST")].code.tolist()

def get_stock_data(end_date, num):
    ris = RIndexStock(ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    start_date = get_day_nday_ago(end_date, num, dformat = "%Y-%m-%d")
    return ris.get_k_data_in_range(start_date, end_date)

def get_stock_pool(end_date, num):
    '''
    更新股票池。该方法在收盘后调用。
    1. 全市场(不包含ST)股票作为基础股票池
    2. 剔除自制股票黑名单中的股票
    3. 剔除过去200交易日总成交额中位数后25%的股票
    4. 剔除总市值中位数在100亿以下的股票
    5. 取25日跌幅前10%的股票作为最终的股票池
    '''
    #黑名单股票
    black_list = get_blacklist()
    #所有单股票
    all_code_list = get_all_codelist()
    all_code_list = list(set(all_code_list).difference(set(black_list)))
    all_df = get_stock_data(end_date, num)
    import pdb
    pdb.set_trace()
    for code, df in all_df.groupby('code'):
        df = df.reset_index(drop = True)
        import pdb
        pdb.set_trace()

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
        x = get_stock_pool(end_date = '2019-04-11', num = 186)
        #start_date = '2005-12-01'
        #end_date   = '2006-12-31'
        #codes = ['000300']  # 股票池
        #main(codes, start_date, end_date)
    except Exception as e:
        traceback.print_exc()
