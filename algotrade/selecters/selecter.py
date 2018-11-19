#coding=utf-8
import const as ct
from cmysql import CMySQL
from log import getLogger
from common import create_redis_obj
class Selecter(object):
    def __init__(self, code, dbinfo, redis_host):
        self.code = code
        self.logger = getLogger(__name__)
        self.dbname = self.get_dbname(code)
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client = CMySQL(dbinfo, dbname = self.dbname, iredis = self.redis)

    def __del__(self):
        self.redis = None
        self.mysql_client = None

    @staticmethod
    def get_dbname(code):
        return "c%s" % code

    def choose(self, *args, **kargs):
        pass
