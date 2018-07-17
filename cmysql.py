#encoding=utf-8
import time
import sqlalchemy
import const as ct
import pandas as pd
from pandas import DataFrame
from log import getLogger
import MySQLdb as db
from sqlalchemy import create_engine
from common import create_redis_obj
from warnings import filterwarnings
filterwarnings('error', category = db.Warning)

log = getLogger(__name__)
ALL_TABLES = 'all_tables'
ALL_TRIGGERS = 'all_triggers'

class CMySQL:
    def __init__(self, dbinfo):
        self.dbinfo = dbinfo
        self.redis = create_redis_obj()
        self.engine = create_engine("mysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8" % dbinfo)

    def get_all_tables(self):
        if self.redis.exists(ALL_TABLES):
            return set(str(table, encoding = "utf8") for table in self.redis.smembers(ALL_TABLES))
        else:
            all_tables = self._get('SHOW TABLES', 'Tables_in_stock')
            for table in all_tables: self.redis.sadd(ALL_TABLES, table)
            return all_tables

    def get_all_triggers(self):
        if self.redis.exists(ALL_TRIGGERS):
            return set(str(table, encoding = "utf8") for table in self.redis.smembers(ALL_TRIGGERS))
        else:
            all_triggers = self._get('SHOW TRIGGERS', 'Trigger')
            for trigger in all_triggers: self.redis.sadd(ALL_TRIGGERS, trigger)
            return all_triggers

    def _get(self, sql, key):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = self.engine.connect()
                df = pd.read_sql(sql, conn)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.info(e)
            finally: 
                if 'conn' in dir(): conn.close()
            if True == res:return set(df[key].tolist()) if not df.empty else set()
        log.error("get all info failed afer try %d times" % ct.RETRY_TIMES)
        return set()

    def set(self, data_frame, table, method = ct.APPEND):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = self.engine.connect()
                data_frame.to_sql(table, conn, if_exists = method, index=False)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.info(e)
            except sqlalchemy.exc.ProgrammingError as e:
                log.debug(e)
            except sqlalchemy.exc.IntegrityError as e:
                log.debug(e)
                res = True
            finally:
                if 'conn' in dir(): conn.close()
            if True == res:return True
        log.error("write to %s failed afer try %d times" % (table, ct.RETRY_TIMES))
        return res 

    def get(self, sql):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = self.engine.connect()
                data = pd.read_sql_query(sql, conn)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                log.debug(e)
            finally:
                if 'conn' in dir(): conn.close()
            if True == res: return data
        log.error("get %s failed afer try %d times" % (sql, ct.RETRY_TIMES))
        return None

    def exec_sql(self, sql):
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
                if 'conn' in dir(): conn.rollback()
            finally:
                if 'cur' in dir(): cur.close()
                if 'conn' in dir(): conn.close()
            if hasSucceed: return True
            time.sleep(ct.SHORT_SLEEP_TIME)
        log.error("%s failed" % sql)
        return False

    def register(self, sql, register):
        if self.exec_sql(sql):
            self.redis.sadd(ALL_TRIGGERS, register)
            return True
        return False

    def create(self, sql, table):
        if self.exec_sql(sql):
            self.redis.sadd(ALL_TABLES, table)
            return True
        return False

if __name__ == '__main__':
    import tushare as ts
    df = ts.get_stock_basics()
    df = df.reset_index(drop = Fasle)
    obj = CMySQL(ct.DB_INFO)
    obj.set(df, 'stock', ct.APPEND)
