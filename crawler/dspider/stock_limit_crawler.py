#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class StockLimitCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.table_name = self.get_tablename()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("create stock limit database failed")
        if not self.create_table(self.table_name): raise Exception("create stock limit table failed")

    @staticmethod
    def get_dbname():
        return "stock"

    @staticmethod
    def get_tablename():
        return "limitup"

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(6) not null,\
                                             price float,\
                                             pchange float,\
                                             prange float,\
                                             concept varchar(200),\
                                             fcb float,\
                                             flb float,\
                                             fdmoney float,\
                                             first_time varchar(20),\
                                             last_time varchar(20),\
                                             open_times varchar(20),\
                                             intensity float,\
                                             PRIMARY KEY (date, code))' % self.table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)
