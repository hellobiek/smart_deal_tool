#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class InvestorCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.table = self.get_table_name()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("init stock database failed")
        if not self.create_table(): raise Exception("init week investor table failed")

    @staticmethod
    def get_dbname():
        return "stock"

    @staticmethod
    def get_table_name():
        return "investor"

    def create_table(self):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             new_investor float,\
                                             final_investor float,\
                                             new_natural_person float,\
                                             new_non_natural_person float,\
                                             final_natural_person float,\
                                             final_non_natural_person float,\
                                             PRIMARY KEY(date))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

class MonthInvestorCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.table = self.get_table_name()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("init stock database failed")
        if not self.create_table(): raise Exception("init month investor table failed")

    @staticmethod
    def get_dbname():
        return "stock"

    @staticmethod
    def get_table_name():
        return "month_investor"

    def create_table(self):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             new_investor float,\
                                             new_natural_person float,\
                                             new_non_natural_person float,\
                                             final_investor float,\
                                             final_natural_person float,\
                                             final_natural_a_person float,\
                                             final_natural_b_person float,\
                                             final_non_natural_person float,\
                                             final_non_natural_a_person float,\
                                             final_non_natural_b_person float,\
                                             final_hold_investor float,\
                                             final_a_hold_investor float,\
                                             final_b_hold_investor float,\
                                             trading_investor float,\
                                             trading_a_investor float,\
                                             trading_b_investor float,\
                                             PRIMARY KEY(date))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)
