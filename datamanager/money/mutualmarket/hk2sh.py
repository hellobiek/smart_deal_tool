#coding=utf-8
from log import getLogger
from combination import Combination
logger = getLogger(__name__)

class HK2SH(Combination):
    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None):
        Combination.__init__(self, dbinfo, code, redis_host)
