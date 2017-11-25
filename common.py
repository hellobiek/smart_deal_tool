#!/usr/bin/python
# coding=utf-8
import sys
import tempfile
import MySQLdb as db
from log import getLogger
from time import sleep
import time
import datetime
from datetime import datetime,timedelta
from crack_bmp import crack_bmp
from warnings import filterwarnings
from const import UTF8, RETRY_TIMES, SHORT_SLEEP_TIME
from const import MARKET_SH,MARKET_SZ,MARKET_CYB,SZ50,HS300,ZZ500,MSCI,MARKET_ALL
filterwarnings('error', category = db.Warning)
log = getLogger(__name__)
def gint(x):
    if x > 9.7:
        return 10
    elif 9 <= x <= 9.7:
        return 9 
    elif x < -9.7:
        return -10
    elif -9.7 <= x <= -9:
        return -9
    else:
        return int(x)

def trace_func(*dargs, **dkargs):
    def wrapper(func):
        def _wrapper(*args, **kargs):
            if 'log' not in dkargs:
                print 'Start %s(%s, %s)...' % (func.__name__, args, kargs)
            else:
                dkargs['log'].info('Start %s(%s, %s)...' % (func.__name__, args, kargs))
            return func(*args, **kargs)
        return _wrapper
    return wrapper

def get_all_tables(dbuser, dbpasswd, dbname, dbhost):
    sql = 'SHOW TABLES'
    tables = []
    try:
        conn = db.connect(host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname,charset=UTF8)
        cur = conn.cursor()
        cur.execute(sql)
        all_tables = cur.fetchall()
        for table in all_tables:
            tables.append(table[0])
        conn.rollback()
    except db.Warning, w:
        log.warn("Warning:%s" % str(w))
    except db.Error, e:
        log.error("Error:%s" % str(e))
    finally:
        if 'cur' in dir():
            cur.close()
        if 'conn' in dir():
            conn.close()
    return tables

def create_table(dbuser, dbpasswd, dbname, dbhost, sql):
    hasSucceed = False
    for i in range(RETRY_TIMES):
        if hasSucceed:
            return True
        try:
            conn = db.connect(host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname,charset=UTF8)
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            hasSucceed = True
        except db.Warning, w:
            log.warn("Warning:%s" % str(w))
            hasSucceed = True
        except db.Error, e:
            log.error("Error:%s" % str(e))
        finally:
            if 'cur' in dir():
                cur.close()
            if 'conn' in dir():
                conn.close()
        sleep(SHORT_SLEEP_TIME)
    return False

def get_verified_code(tmp_buff):
    temp = tempfile.TemporaryFile()
    with open(temp.name, 'wb') as verify_pic:
        verify_pic.write(tmp_buff)
    return crack_bmp().decode_from_file(temp.name)

def is_sub_new_stock(time2Market, timeLimit = 365):
    if time2Market == '0': #for stock has not benn in market
        return False
    if time2Market:
       t = time.strptime(time2Market, "%Y%m%d")
       y,m,d = t[0:3]
       time2Market = datetime(y,m,d)
       if (datetime.today()-time2Market).days < timeLimit:
           return True
    return False

def delta_days(_from, _to):
    _from = time.strptime(_from,"%Y-%m-%d")
    _to = time.strptime(_to,"%Y-%m-%d")
    _from = datetime(_from[0],_from[1],_from[2])
    _to = datetime(_to[0],_to[1],_to[2])
    return (_to - _from).days

def get_market_name(_market):
    if _market == "ALL":
        return MARKET_ALL
    elif _market == "SH":
        return MARKET_SH
    elif _market == "SZ":
        return MARKET_SZ
    elif _market == "CYB":
        return MARKET_CYB
    elif _market == "SZ50":
        return SZ50
    elif _market == "HS300":
        return HS300
    elif _market == "ZZ500":
        return ZZ500
    else:
        return MSCI

def _fprint(obj):
    print "***************************s"
    print obj
    print "***************************e"
