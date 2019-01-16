#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct 
from algotrade.selecters.selecter import Selecter
class NeiChipIntensiveSelecter(Selecter):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        code = ct.SELECTERS_DICT[self.__class__.__name__]
        Selecter.__init__(self, code, dbinfo, redis_host)

    def choose(self, df):
        return df.loc[(df.npercent > 60) & (df.profit < 1)].code.tolist()
