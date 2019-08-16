# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from pyalgotrade import strategy
from algotrade.strategy import gen_broker
class FollowTrendStrategy(strategy.BacktestingStrategy):
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
