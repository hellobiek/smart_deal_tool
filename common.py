#!/usr/bin/python
# coding=utf-8
import sys
import tempfile
import MySQLdb as db
from log import getLogger
from time import sleep
from crack_bmp import crack_bmp
from warnings import filterwarnings
from const import UTF8, RETRY_TIMES, SHORT_SLEEP_TIME
filterwarnings('error', category = db.Warning)
log = getLogger(__name__)
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
        cur.close()
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
            cur.close()
            conn.close()
        sleep(SHORT_SLEEP_TIME)
    return False

def get_verified_code(tmp_buff):
    temp = tempfile.TemporaryFile()
    with open(temp.name, 'wb') as verify_pic:
        verify_pic.write(tmp_buff)
    return crack_bmp().decode_from_file(temp.name)

def _fprint(obj):
    print "***************************s"
    print obj
    print "***************************e"
    sys.exit(0)
