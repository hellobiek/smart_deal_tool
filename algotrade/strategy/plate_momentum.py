# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import json
import numpy as np
import const as ct
from cmysql import CMySQL
from cindex import CIndex
from cstock import CStock
from pandas import DataFrame
from common import create_redis_obj
from industry_info import IndustryInfo
from base.cdate import get_day_nday_ago
from rindustry import RIndexIndustryInfo
from algotrade.feed import dataFramefeed 
from pyalgotrade.technical import highlow, ma
from pyalgotrade.stratanalyzer import returns
from pyalgotrade import plotter, strategy, broker
class PlateMomentumStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instruments, brk, threshold):
        super(PlateMomentumStrategy, self).__init__(feed, brk)
        self.__feed = feed
        self.__position = None
        self.__instruments = instruments
        self.__sma = dict()
        self.__info_dict = dict()
        self.__threshold = dict()
        for element in instruments:
            self.__threshold[element] = threshold
            self.__sma[element] = ma.SMA(feed[element].getCloseDataSeries(), 15)
            self.__info_dict[element] = DataFrame(columns={'date', 'id', 'action', 'quantity', 'price'})
        self.setUseAdjustedValues(False)

    def setInfo(self, order):
        __date = order.getSubmitDateTime()      # 时间
        __action = order.getAction()            # 动作
        __id = order.getId()                    # 订单号
        __instrument = order.getInstrument()    # 股票
        __quantity = order.getQuantity()        # 数量
        __price = order.getAvgFillPrice()
        self.__info_dict[__instrument].at[len(self.__info_dict)] = [__date, __id, __action, __quantity, __price]

    def getInfo(self, instrument):
        return self.__info_dict[instrument]

    def getThreshold(self, instrument):
        return self.__threshold[instrument]

    def getDateTimeSeries(self, instrument = None):
        return self.__feed[instrument].getPriceDataSeries().getDateTimes()

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onBars(self, bars):
        for element in bars.getInstruments():
            price = bars[element].getClose()
            volume = bars[element].getVolume()
            if self.__sma[element][-1] is None: continue
            price = bars[element].getClose()
            if self.__position is None:
                if price < self.__sma[element][-1] * 0.9:
                    shares = 100 * int(self.getBroker().getCash()/(100 * price))
                    self.__position = self.enterLong(element, shares, True)
                    self.info("buy %s %s at ￥%.2f, exists cash:%s" % (element, shares, price, self.getBroker().getCash()))
            else:
                if price > self.__sma[element][-1] * 1.1:
                    if self.__position is not None and not self.__position.exitActive():
                        self.__position.exitMarket()
                        self.info("sell %s %s at ￥%.2f, exists cash:%s" % (element, self.getBroker().getShares(element), price, self.getBroker().getCash()))

def max_turnover(df):
    return True if len(df.loc[df.turnover > 6]) < 3 else False

def average_amount_volume(df):
    MONEY_LIMIT = 100000000
    mean_value = np.mean(df.amount)
    median_value = np.median(df.amount)
    return median_value > MONEY_LIMIT and mean_value > MONEY_LIMIT

def large_down_time(df):
    num = len(df.loc[df.pchange < -8])
    return True if num < 3 else False

def game_kline_without_high_turnover(df):
    df['preppercent'] = df['ppercent'].shift(1)
    df['kline'] = df['ppercent'] - df['preppercent']
    return len(df.loc[(df.kline > 18) & (df.turnoever < 3)])

#选股的指标
    #condition 1: 平均成交额 > 1亿
    #condition 3: 基础浮动盈利
    #condition 3: 短期浮动盈利
    #condition 4: 相对大盘的涨跌
    #condition 5: 连续暴跌的次数的最大次数
    #condition 5: 逆势飘红的次数 
    #condition 6: 近邻筹码的平均比例
    #condition 7: 获利筹码的平均比例
    #condition 8: 博弈K线穿越>10%筹码，且换手率小于3%的次数
    #condition 9: 博弈K线穿越>10%筹码，且换手率大于3%的次数
def choose_stock(code_list, start_date, end_date):
    state_dict = dict()
    state_dict[ct.FL] = list()
    state_dict[ct.JL] = list()
    state_dict[ct.QL] = list()
    state_dict[ct.KL] = list()
    good_code_list    = list()
    #stock_info = DataFrame(columns={'code', 'ppercent', 'npercent', 'sai', 'sri', 'pchange'})
    stock_info = DataFrame()
    for code in code_list:
        cstock_obj = CStock(code, redis_host = '127.0.0.1')
        df_profit = cstock_obj.get_base_floating_profit_in_range(start_date, end_date)
        if df_profit.profit.mean() > 2:
            state_dict[ct.FL].append(code)
        elif df_profit.profit.mean() < -2:
            state_dict[ct.KL].append(code)
        elif df_profit.profit.mean() >= -2 and df_profit.profit.mean() <= 0:
            state_dict[ct.QL].append(code)
        else:
            state_dict[ct.JL].append(code)
        df = cstock_obj.get_k_data_in_range(start_date, end_date)
        if df is None or df.empty or len(df) < 55: continue
        if average_amount_volume(df) and large_down_time(df) and max_turnover(df):
            good_code_list.append(code)
    return state_dict, good_code_list

#edate日的状态
    #涨幅最大的板块
    #跌幅最大的板块
    #资金变化最大的板块
    #板块的相对大盘强弱
    #板块的逆势大盘上涨的强弱
def choose_plate(edate = '2016-10-11', ndays = 90):
    rindustry_info_client = RIndexIndustryInfo(redis_host='127.0.0.1')
    today_industry_df = rindustry_info_client.get_k_data(edate)
    pchange_df = today_industry_df.sort_values(by = 'pchange', ascending = False).head(3)
    mchange_df = today_industry_df.sort_values(by = 'mchange', ascending = False).head(3)
    plate_code_list = list(set(pchange_df.code.tolist()).intersection(pchange_df.code.tolist()))
    if len(plate_code_list) == 0: 
        logger.info("no interested plate for date:%s" % edate)
        return list()
    sdate = get_day_nday_ago(edate, ndays, '%Y-%m-%d')
    #get sh index data
    sh_index_obj = CIndex('000001', redis_host='127.0.0.1')
    sh_index_info = sh_index_obj.get_k_data_in_range(sdate, edate)
    sh_index_pchange = 100 * (sh_index_info.loc[len(sh_index_info) - 1, 'close'] -  sh_index_info.loc[0, 'preclose']) / sh_index_info.loc[0, 'preclose']
    #get industry data
    all_industry_df = rindustry_info_client.get_k_data_in_range(sdate, edate)
    all_industry_df = all_industry_df.loc[all_industry_df.code.isin(plate_code_list)]
    industry_static_info = DataFrame(columns={'code', 'sai', 'pchange', ct.KL, ct.QL, ct.JL, ct.FL})
    #choose better industry
    redisobj = create_redis_obj("127.0.0.1") 
    today_industry_info = IndustryInfo.get(redisobj)
    for code, industry in all_industry_df.groupby('code'):
        industry = industry.reset_index(drop = True)
        industry['sri'] = 0
        industry['sri'] = industry['pchange'] - sh_index_info['pchange']
        industry['sai'] = 0
        industry.at[(industry.pchange > 0) & (sh_index_info.pchange < 0), 'sai'] = industry.loc[(industry.pchange > 0) & (sh_index_info.pchange < 0), 'sri']
        industry_sai = len(industry.loc[industry.sai > 0])
        industry_pchange = 100 * (industry.loc[len(industry) - 1, 'close'] -  industry.loc[0, 'preclose']) / industry.loc[0, 'preclose']
        code_list = json.loads(today_industry_info.loc[today_industry_info.code == code, 'content'].values[0])
        info_dict, good_code_list = choose_stock(code_list, sdate, edate)
        industry_static_info = industry_static_info.append(DataFrame([[code, industry_sai, industry_pchange, info_dict[ct.KL], info_dict[ct.QL], info_dict[ct.JL], info_dict[ct.FL]]], columns = ['code', 'sai', 'pchange', ct.KL, ct.QL, ct.JL, ct.FL]), sort = 'True')
    industry_static_info = industry_static_info.reset_index(drop = True)
    industry_static_info = industry_static_info.sort_values(by=['pchange'], ascending=False)
    return good_code_list

def plate_momentum(mode = ct.PAPER_TRADING, start_date = '2018-03-01', end_date = '2018-10-28'):
    if mode == ct.PAPER_TRADING:
        threshold = 100000
        feed = dataFramefeed.Feed()
        instruments = choose_plate()
        if len(instruments): return 0
        for code in instruments:
            data = CStock(code, should_create_influxdb = False, should_create_mysqldb = False).get_k_data_in_range(start_date, end_date)
            data = data.set_index('date')
            feed.addBarsFromDataFrame(code, data)

        # broker setting
        # broker commission类设置
        broker_commission = broker.backtesting.TradePercentage(0.002)
        # fill strategy设置
        fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = 1.0)
        sli_stra = broker.slippage.NoSlippage()
        fill_stra.setSlippageModel(sli_stra)
        # 完善broker类
        brk = broker.backtesting.Broker(threshold * len(instruments), feed, broker_commission)
        brk.setFillStrategy(fill_stra)

    myStrategy = PlateMomentumStrategy(feed, instruments, brk, threshold)

    # Attach a returns analyzers to the strategy.
    returnsAnalyzer = returns.Returns()
    myStrategy.attachAnalyzer(returnsAnalyzer)

    # Attach the plotter to the strategy.
    plt = plotter.StrategyPlotter(myStrategy)

    # Plot the simple returns on each bar.
    plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())

    myStrategy.run()
    myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())

    plt.plot()

if __name__ == '__main__':
    plate_momentum()
