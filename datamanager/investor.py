#coding=utf-8
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
class CInvestor(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.table = self.get_table_name()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)

    @staticmethod
    def get_dbname():
        return "stock"

    @staticmethod
    def get_table_name():
        return "month_investor"

    def get_data(self, mdate = None):
        table_name = self.get_table_name()
        if mdate is not None:
            sql = "select * from %s where date=\"%s\"" %(table_name, mdate)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    def get_data_in_range(self, start_date, end_date):
        table_name = self.get_table_name()
        sql = "select * from %s where date between \"%s\" and \"%s\"" %(table_name, start_date, end_date)
        return self.mysql_client.get(sql)
