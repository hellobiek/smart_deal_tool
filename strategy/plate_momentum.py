# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import json
from cmysql import CMySQL
from cindex import CIndex
from cstock import CStock
from industry_info import IndustryInfo
from pandas import DataFrame
from base.feed import dataFramefeed 
from rindustry import RIndexIndustryInfo
from common import get_day_nday_ago, get_dates_array, create_redis_obj
from technical import bfp, gkr, prt, pvh, rat, rolling_peak
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
                    self.info("before buy %s %s at ￥%.2f, exists cash:%s" % (element, shares, price, self.getBroker().getCash()))
                    self.__position = self.enterLong(element, shares, True)
                    self.info("after buy %s %s at ￥%.2f, exists cash:%s" % (element, shares, price, self.getBroker().getCash()))
            else:
                if price > self.__sma[element][-1] * 1.1:
                    if self.__position is not None and not self.__position.exitActive():
                        self.info("before sell %s %s at ￥%.2f, exists cash:%s" % (element, self.getBroker().getShares(element), price, self.getBroker().getCash()))
                        self.__position.exitMarket()
                        self.info("after sell %s %s at ￥%.2f, exists cash:%s" % (element, self.getBroker().getShares(element), price, self.getBroker().getCash()))

#获取sdate日期涨幅最大的板块，跌幅最大的板块，资金变化最大的板块
    #获取板块的相对大盘强弱
    #获取板块的逆势大盘上涨的强弱

#分析该板块中的个股:
    #逆势飘红的强度
    #筹码密集的程度
    #相对大盘涨跌的强度
    #博弈K线无量长阳的状态
    #换手率的中位数和平均数
    #成交额的中位数和平均数
    #相对盈利状态，相对60日成本均线的状态。
    #绝对盈利状态，处于下成本区，上成本区，盈利区域。

KL = 0
QL = 1
JL = 2
FL = 3
def get_stock_condition(code_list, start_date, end_date):
    state_dict = dict()
    state_dict[FL] = list()
    state_dict[JL] = list()
    state_dict[QL] = list()
    state_dict[KL] = list()
    for code in code_list:
        df = CStock(code, redis_host = '127.0.0.1').get_base_floating_profit_in_range(start_date, end_date)
        if df.profit.mean() > 2:
            state_dict[FL].append(code)
        elif df.profit.mean() < -2:
            state_dict[KL].append(code)
        elif df.profit.mean() >= -2 and df.profit.mean() <= 0:
            state_dict[QL].append(code)
        else:
            state_dict[JL].append(code)
    return state_dict

def choose_plate(name = None, edate = '2016-10-11', ndays = 90):
    sdate = get_day_nday_ago(edate, ndays, '%Y-%m-%d')
    #get sh index data
    sh_index_obj = CIndex('000001', redis_host='127.0.0.1')
    sh_index_info = sh_index_obj.get_k_data_in_range(sdate, edate)
    sh_index_pchange = 100 * (sh_index_info.loc[len(sh_index_info) - 1, 'close'] -  sh_index_info.loc[0, 'preclose']) / sh_index_info.loc[0, 'preclose']
    #get industry data
    rindustry_info_client = RIndexIndustryInfo(redis_host='127.0.0.1')
    all_industry_df = rindustry_info_client.get_k_data_in_range(sdate, edate)
    industry_static_info = DataFrame(columns={'code', 'sai', 'pchange'})

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
        industry_static_info = industry_static_info.append(DataFrame([[code, industry_sai, industry_pchange]], columns = ['code', 'sai', 'pchange']), sort = 'True')
        industry_static_info = industry_static_info.reset_index(drop = True)
        if code == '880491':
            code_list = json.loads(today_industry_info.loc[today_industry_info.code == code, 'content'].values[0])
            info_dict = get_stock_condition(code_list, sdate, edate)
            import pdb
            pdb.set_trace()
    industry_static_info = industry_static_info.sort_values(by=['pchange'], ascending=False)
    import pdb
    pdb.set_trace()

def plate_momentum(mode = ct.PAPER_TRADING, start_date = '2018-03-01', end_date = '2018-10-28'):
    if mode == ct.PAPER_TRADING:
        threshold = 100000
        feed = dataFramefeed.Feed()
        instruments = choose_plate()
        for code in instruments:
            data = CStock(code, should_create_influxdb = False, should_create_mysqldb = False).get_k_data_in_range(start_date, end_date)
            data = data.set_index('date')
            feed.addBarsFromDataFrame(code, data)

        # broker setting
        # broker commission类设置
        broker_commission = broker.backtesting.TradePercentage(0.002)
        # fill strategy设置
        #fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = 1.0)
        #sli_stra = broker.slippage.NoSlippage()
        #fill_stra.setSlippageModel(sli_stra)
        # 完善broker类
        brk = broker.backtesting.Broker(threshold * len(instruments), feed, broker_commission)
        #brk.setFillStrategy(fill_stra)

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
