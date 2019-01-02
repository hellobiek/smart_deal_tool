#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct 
from algotrade.selecters.selecter import Selecter
class BullMoreBearLessSelecter(Selecter):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        code = ct.SELECTERS_DICT[self.__class__.__name__]
        Selecter.__init__(self, code, dbinfo, redis_host)

    def choose(self, all_stock_df):
        code_list = list()
        for code, df in all_stock_df.groupby('code'):
            if len(df[df.pchange > 0]) > len(df[df.pchange < 0]):
                code_list.append(code)
        return code_list
            
