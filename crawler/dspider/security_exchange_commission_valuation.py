#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import pandas as pd
from pathlib import Path
from cmysql import CMySQL
from common import create_redis_obj
SEC_COLUMNS = ['code', 'name', 'mind_code', 'mind_name', 'dind_code', 'dind_name', 'pe', 'ttm', 'pb', 'dividend']
SEC_COLUMN_DICT = {'date': str, 'code': str, 'name': str, 'mind_code': str, 'mind_name': str, 'dind_code': str, 'dind_name': str, 'pe': float, 'ttm': float, 'pb': float, 'dividend': float}
class SecurityExchangeCommissionValuationCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, shoud_create_db = False):
        if shoud_create_db:
            self.dbname = self.get_dbname()
            self.table_name = self.get_tablename()
            self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
            self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
            if not self.mysql_client.create_db(self.dbname): raise Exception("create security exchange commission database failed")
            if not self.create_table(self.table_name): raise Exception("create valuation table failed")

    @staticmethod
    def get_dbname():
        return "security_exchange_commission"

    @staticmethod
    def get_tablename():
        return "valuation"

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             name varchar(50),\
                                             pe float,\
                                             ttm float,\
                                             pb float,\
                                             dividend float,\
                                             PRIMARY KEY(date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get_k_data(self, mdate):
        file_name = "{}.csv".format(mdate)
        file_path = Path(ct.SECURITY_EXCHANGE_COMMISSION_INDUSTRY_VALUATION_STOCK_PATH) / file_name
        if not file_path.exists(): return pd.DataFrame()
        df = pd.read_csv(file_path, sep = ',', dtype = SEC_COLUMN_DICT)
        SEC_COLUMNS.append('date')
        df = df[SEC_COLUMNS]
        return df
