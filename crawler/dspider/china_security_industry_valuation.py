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
CSI_COLUMNS = ['code', 'name', 'pind_code', 'pind_name', 'sind_code', 'sind_name',
               'tind_code', 'tind_name', 'find_code', 'find_name', 'pe', 'ttm', 'pb',
               'dividend', 'date']
CSI_COLUMN_DICT = {'code': str, 'name': str, 'pind_code': str, 'pind_name': str, 
                   'sind_code': str, 'sind_name': str, 'tind_code': str, 'tind_name': str,
                   'find_code': str, 'find_name': str, 'pe': float, 'ttm': float, 'pb': float,
                   'dividend': float, 'date': str}
class ChinaSecurityIndustryValuationCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, shoud_create_db = False):
        if shoud_create_db:
            self.dbname = self.get_dbname()
            self.table_name = self.get_tablename()
            self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
            self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
            if not self.mysql_client.create_db(self.dbname): raise Exception("create china security database failed")
            if not self.create_table(self.table_name): raise Exception("create valuation table failed")

    @staticmethod
    def get_dbname():
        return "china_security_industry"

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
        file_path = Path(ct.CHINA_SECURITY_INDUSTRY_VALUATION_STOCK_PATH) / file_name
        if not file_path.exists(): return pd.DataFrame()
        df = pd.read_csv(file_path, sep = ',', dtype = CSI_COLUMN_DICT)
        CSI_COLUMNS.append('date')
        df = df[CSI_COLUMNS]
        return df
