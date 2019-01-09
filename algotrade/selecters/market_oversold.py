#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct 
import numpy as np
from algotrade.selecters.selecter import Selecter
class MarketOversoldJudger(Selecter):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        code = ct.SELECTERS_DICT[self.__class__.__name__]
        Selecter.__init__(self, code, dbinfo, redis_host)

    def get_pchange(self, df):
        df = df.sort_values(by = 'date', ascending= True)
        close_price = df['close'].tolist()[-1]
        open_price  = df['open'].tolist()[0]
        return 100 * (close_price - open_price) / open_price

    def judge(self, stock_df):
        oversold_num = len(stock_df[(np.log(stock_df['close']) - np.log(stock_df['sprice'])) / np.log(0.9) > 1.5])
        total_num = len(stock_df)
        return oversold_num / total_num > 0.34
