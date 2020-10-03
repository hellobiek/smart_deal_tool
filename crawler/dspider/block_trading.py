# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class BlockTradingCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, shoud_create_db = False):
        if shoud_create_db:
            self.dbname = "block_trading"
            self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
            self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
            if not self.mysql_client.create_db(self.dbname): raise Exception("create china security database failed")
