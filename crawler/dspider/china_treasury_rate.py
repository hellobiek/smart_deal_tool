#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class ChinaTreasuryRateCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.table_name = self.get_tablename()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("create china treasury database failed")
        if not self.create_table(self.table_name): raise Exception("create rate table failed")

    @staticmethod
    def get_dbname():
        return "china_treasury"

    @staticmethod
    def get_tablename():
        return "yield"

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             name varchar(50),\
                                             month3 float,\
                                             month6 float,\
                                             year1 float,\
                                             year3 float,\
                                             year5 float,\
                                             year7 float,\
                                             year10 float,\
                                             year30 float,\
                                             PRIMARY KEY(date, name))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)
