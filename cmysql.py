#encoding=utf-8
import time
import copy
import pymysql
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
ALL_DATABASES = 'all_databases'
ALL_TRIGGERS = 'all_triggers'

class CMySQL:
    def __init__(self, dbinfo, dbname = 'stock', iredis = None):
        self.dbinfo = dbinfo
        self.dbname = dbname
        self.redis = create_redis_obj() if iredis is None else iredis
        self.engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (self.dbinfo['user'], self.dbinfo['password'], self.dbinfo['host'], self.dbname), pool_size=0 , max_overflow=-1, pool_recycle=20, pool_timeout=5, connect_args={'connect_timeout': 3})

    def __del__(self):
        self.redis = None
        self.engine = None

    def changedb(self, dbname = 'stock'):
        self.dbname = dbname
        self.engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (self.dbinfo['user'], self.dbinfo['password'], self.dbinfo['host'], self.dbname), pool_size=0 , max_overflow=-1, pool_recycle=20, pool_timeout=5, connect_args={'connect_timeout': 3})

    def get_all_databases(self):
        if self.redis.exists(ALL_DATABASES):
            return set(str(dbname, encoding = "utf8") for dbname in self.redis.smembers(ALL_DATABASES))
        else:
            all_dbs = self._get_all_databses()
            for _db in all_dbs: self.redis.sadd(ALL_DATABASES, _db)
            return all_dbs

    def get_all_tables(self):
        if self.redis.exists(self.dbname):
            return set(str(table, encoding = "utf8") for table in self.redis.smembers(self.dbname))
        else:
            all_tables = self._get('SHOW TABLES', 'Tables_in_%s' % self.dbname)
            for table in all_tables: self.redis.sadd(self.dbname, table)
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
            except Exception as e:
                log.debug(e)
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
                log.debug(e)
            except sqlalchemy.exc.ProgrammingError as e:
                log.debug(e)
            except sqlalchemy.exc.IntegrityError as e:
                log.debug(e)
                res = True
            finally:
                if 'conn' in dir(): conn.close()
            if True == res:return True
        log.error("write to %s-%s failed afer try %d times" % (self.dbname, table, ct.RETRY_TIMES))
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
                if 'conn' in dir(): conn.close()
            except Exception as e:
                log.error(e)
                if 'conn' in dir(): conn.close()
            if True == res: return data
        log.error("%s get %s failed afer try %d times" % (self.dbname, sql, ct.RETRY_TIMES))
        return None

    def exec_sql(self, sql):
        hasSucceed = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = db.connect(host=self.dbinfo['host'],user=self.dbinfo['user'],passwd=self.dbinfo['password'],db=self.dbname,charset=ct.UTF8,connect_timeout=3)
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                hasSucceed = True
            except db.Warning as w:
                if 'conn' in dir(): conn.rollback()
                log.debug("warning:%s" % str(w))
                hasSucceed = True
            except db.Error as e:
                if 'conn' in dir(): conn.rollback()
                log.debug("error:%s" % str(e))
            finally:
                if 'cur' in dir(): cur.close()
                if 'conn' in dir(): conn.close()
            if hasSucceed: return True
            if ct.RETRY_TIMES > 1: 
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
            self.redis.sadd(self.dbname, table)
            return True
        return False

    def delete(self, table):
        sql = 'drop table %s' % table
        if self.exec_sql(sql):
            self.redis.srem(self.dbname, table)
            return True
        return False

    def _get_all_databses(self):
        db_list = list()
        try:
            conn = pymysql.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'],connect_timeout=3,read_timeout=5,write_timeout=10)
            cursor = conn.cursor()
            cursor.execute("show databases;")
            db_tuple = cursor.fetchall()
            cursor.close()
            conn.commit()
            db_list = [tmp_db[0] for tmp_db in db_tuple]
        except Exception as e:
            if 'conn' in dir(): conn.rollback()
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        return db_list

    def delete_db(self, dbname):
        if self.redis.exists(ALL_DATABASES) and dbname not in set(str(tdb, encoding = "utf8") for tdb in self.redis.smembers(ALL_DATABASES)):
            return True
        res = False
        try:
            conn = pymysql.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'], charset='utf8')
            cursor = conn.cursor()
            cursor.execute("drop database if exists %s" % dbname)
            cursor.close()
            conn.commit()
            res = True
        except Exception as e:
            if 'conn' in dir(): conn.rollback()
            res = False
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        if res == True: self.redis.srem(ALL_DATABASES, dbname)
        return res

    def create_db(self, dbname):
        if self.redis.exists(ALL_DATABASES) and dbname in set(str(tdb, encoding = "utf8") for tdb in self.redis.smembers(ALL_DATABASES)):
            return True
        res = False
        try:
            conn = pymysql.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'], charset='utf8')
            cursor = conn.cursor()
            cursor.execute("create database if not exists %s" % dbname)
            cursor.close()
            conn.commit()
            res = True
        except Exception as e:
            if 'conn' in dir(): conn.rollback()
            res = False
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        if res == True: self.redis.sadd(ALL_DATABASES, dbname)
        return res
