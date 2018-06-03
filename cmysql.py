#encoding=utf-8
import time
import sqlalchemy
import const as ct
import pandas as pd
from pandas import DataFrame
from log import getLogger
log = getLogger(__name__)
import MySQLdb as db
from sqlalchemy import create_engine
from warnings import filterwarnings
filterwarnings('error', category = db.Warning)

class CMySQL:
    def __init__(self, dbinfo):
        self.dbinfo = dbinfo
        self.engine = create_engine("mysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8" % dbinfo)

    def get_all_tables(self):
        res = False
        sql = 'SHOW TABLES'
        for i in range(ct.RETRY_TIMES):
            try:
                df = pd.read_sql_query(sql, self.engine)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.debug(e)
            if True == res:return df['Tables_in_stock'].tolist() if not df.empty else list()
        log.error("get all tables failed afer try %d times" % ct.RETRY_TIMES)
        return None 

    def set(self, data_frame, table, method = ct.REPLACE):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                data_frame.to_sql(table, self.engine, if_exists = method, index=False)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.debug(e)
            if True == res:return True
        log.error("write to %s failed afer try %d times" % (table, ct.RETRY_TIMES))
        return res 

    def get(self, sql):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                data = pd.read_sql_query(sql, self.engine)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.debug(e)
            if True == res: return data
        log.error("get %s failed afer try %d times" % (sql, ct.RETRY_TIMES))
        return None

    def create(self, sql):
        hasSucceed = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = db.connect(host=self.dbinfo['host'],user=self.dbinfo['user'],passwd=self.dbinfo['password'],db=self.dbinfo['database'],charset=ct.UTF8)
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                hasSucceed = True
            except db.Warning as w:
                log.debug("warning:%s" % str(w))
                hasSucceed = True
            except db.Error as e:
                log.debug("error:%s" % str(e))
            finally:
                if 'cur' in dir(): cur.close()
                if 'conn' in dir(): conn.close()
            if hasSucceed: return True
            time.sleep(ct.SHORT_SLEEP_TIME)
        log.error("create %s failed" % sql)
        return False

if __name__ == '__main__':
    obj = CMySQL(ct.DB_INFO)
    sql = "select * from info"
    obj.create(sql)
