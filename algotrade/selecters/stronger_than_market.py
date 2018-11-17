#coding=utf-8
from selecters.selecter import Selecter
class StrongerThanMarket(Selecter):
    def __init__(self, code, name, dbinfo, redis_host)
        Selecter.__init__(self, code, name, dbinfo = ct.DB_INFO, redis_host = None)

    def choose(self, *args, **kargs):
        pass
