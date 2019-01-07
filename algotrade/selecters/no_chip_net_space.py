#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct 
import numpy as np
from algotrade.selecters.selecter import Selecter
class NoChipNetSpaceSelecter(Selecter):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        code = ct.SELECTERS_DICT[self.__class__.__name__]
        Selecter.__init__(self, code, dbinfo, redis_host)

    def choose(self, stock_df):
        #个股超跌 + 紧邻无筹码的判断
        delta = (np.log(stock_df['aprice']) - np.log(stock_df['sprice'])) / np.log(0.9)
        return stock_df.loc[(delta > 1) & (stock_df.npercent < 20)].code.tolist()
