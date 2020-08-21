# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class MarginCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, shoud_create_db = False):
        if shoud_create_db:
            self.dbname = self.get_dbname()
            self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
            self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
            if not self.mysql_client.create_db(self.dbname): raise Exception("create china security database failed")

    @staticmethod
    def get_dbname():
        return "margin"

    @staticmethod
    def get_table_name(cdate):
        cdates = cdate.split('-')
        return "margin_day_{}_{}".format(cdates[0], (int(cdates[1])-1)//3 + 1)

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             rzye float,\
                                             rzmre float,\
                                             rzche float,\
                                             rqye float,\
                                             rqyl float,\
                                             rqmcl float,\
                                             rqchl float,\
                                             rzrqye float,\
                                             PRIMARY KEY (date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)
