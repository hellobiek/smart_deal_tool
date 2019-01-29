#encoding=utf-8
import time
import pymysql
import sqlalchemy
import const as ct
import pandas as pd
import MySQLdb as db
from log import getLogger
from common import create_redis_obj
from warnings import filterwarnings
from sqlalchemy import create_engine
filterwarnings('error', category = db.Warning)
ALL_DATABASES = 'all_databases'
ALL_TRIGGERS = 'all_triggers'
logger = getLogger(__name__)
class CMySQL:
    def __init__(self, dbinfo, dbname = 'stock', iredis = None):
        self.dbinfo = dbinfo
        self.dbname = dbname
        self.redis  = create_redis_obj() if iredis is None else iredis
        self.engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (self.dbinfo['user'], self.dbinfo['password'], self.dbinfo['host'], self.dbname), pool_size=0 , max_overflow=-1, pool_recycle=20, pool_timeout=5, connect_args={'connect_timeout': 3})

    def __del__(self):
        self.redis = None
        self.engine = None

    def changedb(self, dbname = 'stock'):
        self.dbname = dbname
        self.engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (self.dbinfo['user'], self.dbinfo['password'], self.dbinfo['host'], self.dbname), pool_size=0 , max_overflow=-1, pool_recycle=20, pool_timeout=5, connect_args={'connect_timeout': 3})

    def get_all_databases(self):
        if self.redis.exists(ALL_DATABASES):
            return set(dbname.decode() for dbname in self.redis.smembers(ALL_DATABASES))
        else:
            all_dbs = self._get_all_databses()
            for _db in all_dbs: self.redis.sadd(ALL_DATABASES, _db)
            return all_dbs

    def get_all_tables(self):
        if self.redis.exists(self.dbname):
            return set(table.decode() for table in self.redis.smembers(self.dbname))
        else:
            all_tables = self._get('SHOW TABLES', 'Tables_in_%s' % self.dbname.lower())
            for table in all_tables: self.redis.sadd(self.dbname, table)
            return all_tables

    def get_all_triggers(self):
        if self.redis.exists(ALL_TRIGGERS):
            return set(table.decode() for table in self.redis.smembers(ALL_TRIGGERS))
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
                logger.info(e)
            except Exception as e:
                logger.debug(e)
            finally: 
                if 'conn' in dir(): conn.close()
            if True == res:return set(df[key].tolist()) if not df.empty else set()
        logger.error("get all info failed afer try %d times" % ct.RETRY_TIMES)
        return set()

    def create_update_cols_query(self, table, mdict, kdict):
        '''
        only update some columns in dataframe in mysql
        '''
        query       = ''
        placeholder = ', '.join('{}=%s'.format(k) for k in mdict)
        conditon    = ', '.join('{}=%s'.format(k) for k in kdict)
        query       = "UPDATE {} SET {} WHERE {};".format(table, placeholder, condition)
        query.split()
        query       = ' '.join(query.split())
        return query

    def update_cols(self, df, table, columns, pri_cols):
        #only update entire columns
        res = True
        try:
            conn = db.connect(host=self.dbinfo['host'],user=self.dbinfo['user'],passwd=self.dbinfo['password'],db=self.dbname,charset=ct.UTF8,connect_timeout=3)
            cur = conn.cursor()
            key_values = df[pri_cols].to_dict(orient = 'records')
            insert_values = df[columns].to_dict(orient = 'records')
            for row in insert_values:
                sql = self.create_update_cols_query(table, columns, pri_cols)
                cur.execute(sql, row)
                conn.commit()
            cur.execute(sql, row)
            conn.commit()
        except Exception as e:
            logger.info(e)
            if 'conn' in dir(): conn.rollback()
            res = False
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        return res

    def create_upsert_query(self, table, columns, pri_keys):
        query       = ''
        cols        = ', '.join(['{}'.format(col) for col in columns])
        placeholder = ', '.join(['%({})s'.format(col) for col in columns])
        updates     = ', '.join(['{}=VALUES({})'.format(col, col) for col in columns])
        query       = "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {};".format(table, cols, placeholder, updates)
        query.split()
        query       = ' '.join(query.split())
        return query

    def upsert(self, df, table, pri_keys = list()):
        #if data in mysql, udate to new value,
        #if not in mysql, just append to mysql
        columns = df.columns.tolist()
        insert_items = df.to_dict(orient = 'records')
        sql = self.create_upsert_query(table, columns, pri_keys)
        return self.executemany(sql, params = insert_items)

    def delsert(self, df, table):
        if self.exec_sql("truncate table %s;" % table):
            return self.set(df, table)
        return False
   
    def executemany(self, sql, params = None):
        res = True
        try:
            conn = db.connect(host = self.dbinfo['host'], user = self.dbinfo['user'], passwd = self.dbinfo['password'], db = self.dbname, charset = ct.UTF8, connect_timeout = 30)
            cur = conn.cursor()
            cur.executemany(sql, params)
            conn.commit()
        except Exception as e:
            logger.info(e)
            if 'conn' in dir(): conn.rollback()
            res = False
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        return res

    def create_update_query(self, table):
        query       = ''
        cols        = ', '.join(['{}'.format(col) for col in columns])
        placeholder = ', '.join(['%({})s'.format(col) for col in columns])
        updates     = ', '.join(['{}=%({})s'.format(col, col) for col in columns])
        query       = "UPDATE {} SET {} WHERE {} IN ({0});".format(table, cols, placeholder, updates)
        query.split()
        query       = ' '.join(query.split())
        return query

    def update(self, df, table, columns, pri_keys):
        #first remove the duplicated values, then add the new value in df
        update_items = df.to_dict(orient = 'records')
        query = self.create_update_query(table)
        return self.executemany(query, params = update_items)

    def set(self, data_frame, table):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = self.engine.connect()
                data_frame.to_sql(table, conn, if_exists = ct.APPEND, index=False)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                logger.debug(e)
            except sqlalchemy.exc.ProgrammingError as e:
                logger.debug(e)
            except sqlalchemy.exc.IntegrityError as e:
                logger.debug("duplicated item:%s" % e)
                res = True
            finally:
                if 'conn' in dir(): conn.close()
            if True == res: return True
        logger.error("write to db:%s, table:%s failed afer try %d times" % (self.dbname, table, ct.RETRY_TIMES))
        return res 

    def get(self, sql):
        res = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = self.engine.connect()
                data = pd.read_sql_query(sql, conn)
                res = True
            except sqlalchemy.exc.OperationalError as e:
                logger.debug(e)
                if 'conn' in dir(): conn.close()
            except Exception as e:
                logger.error(e)
                if 'conn' in dir(): conn.close()
            if True == res: return data
        logger.error("%s %s failed afer try %d times" % (self.dbname, sql, ct.RETRY_TIMES))
        return None

    def exec_sql(self, sql, params = None):
        hasSucceed = False
        for i in range(ct.RETRY_TIMES):
            try:
                conn = db.connect(host=self.dbinfo['host'],user=self.dbinfo['user'],passwd=self.dbinfo['password'],db=self.dbname,charset=ct.UTF8,connect_timeout=3)
                cur = conn.cursor()
                cur.execute(sql, params)
                conn.commit()
                hasSucceed = True
            except db.Warning as w:
                if 'conn' in dir(): conn.rollback()
                logger.debug("warning:%s" % str(w))
                hasSucceed = True
            except db.Error as e:
                if 'conn' in dir(): conn.rollback()
                logger.debug("error:%s" % str(e))
            finally:
                if 'cur' in dir(): cur.close()
                if 'conn' in dir(): conn.close()
            if hasSucceed: return True
            if ct.RETRY_TIMES > 1: 
                time.sleep(ct.SHORT_SLEEP_TIME)
        logger.error("%s failed" % sql)
        return False

    def register(self, sql, register):
        if self.exec_sql(sql):
            self.redis.sadd(ALL_TRIGGERS, register)
            return True
        return False

    def create(self, sql, table):
        if self.exec_sql(sql):
            return self.redis.sadd(self.dbname, table)
        return False

    def delete(self, table):
        sql = 'drop table %s' % table
        self.exec_sql(sql)
        self.redis.srem(self.dbname, table)
        self.redis.delete(table)
        return True

    def _get_all_databses(self):
        db_list = list()
        try:
            conn = db.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'],connect_timeout=3,read_timeout=5,write_timeout=10)
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

    def delete_db(self, dbname = None):
        if dbname is None:dbname = self.dbname 
        try:
            conn = db.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'], charset=ct.UTF8)
            cursor = conn.cursor()
            cursor.execute("drop database if exists %s" % dbname)
            cursor.close()
            conn.commit()
        except Exception as e:
            if 'conn' in dir(): conn.rollback()
        finally:
            if 'curosr' in dir(): cursor.close()
            if 'conn' in dir(): conn.close()
        for tab in set(tdb.decode() for tdb in self.redis.smembers(dbname)):
            self.redis.delete(tab)
        self.redis.delete(dbname)
        self.redis.srem(ALL_DATABASES, dbname)
        return True

    def create_db(self, dbname = None):
        if dbname is None: dbname = self.dbname
        if self.redis.exists(ALL_DATABASES) and dbname in set(tdb.decode() for tdb in self.redis.smembers(ALL_DATABASES)):
            return True
        res = False
        try:
            conn = db.connect(host=self.dbinfo['host'], user=self.dbinfo['user'], passwd=self.dbinfo['password'], charset=ct.UTF8)
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

if __name__ == '__main__':
    cmy = CMySQL(ct.DB_INFO)
    print(cmy._get_all_databses())
