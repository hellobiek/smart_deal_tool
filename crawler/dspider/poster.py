# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import pymysql
import pymysql.cursors
import const as ct
from log import getLogger
from twisted.enterprise import adbapi
from hkex import HkexCrawler
from investor import InvestorCrawler
from pymysql.err import OperationalError, InterfaceError, DataError, InternalError, IntegrityError

logger = getLogger(__name__)

class Poster(object):
    def __init__(self, item):
        self.item = item
        self.table = ''

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql(self.table)
        cursor.execute(insert_sql, params)

    def store(self):
        pass

def get_hk_dbname(market, direction):
    dbname = ''
    if market == 'sse' and direction == 'north':
        dbname = HkexCrawler.get_dbname(ct.SH_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL)
    elif market == 'sse' and direction == 'south':
        dbname = HkexCrawler.get_dbname(ct.HK_MARKET_SYMBOL, ct.SH_MARKET_SYMBOL)
    elif market == 'szse' and direction == 'south':
        dbname = HkexCrawler.get_dbname(ct.HK_MARKET_SYMBOL, ct.SZ_MARKET_SYMBOL)
    else:
        dbname = HkexCrawler.get_dbname(ct.SZ_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL)
    return dbname

class HkexTradeOverviewPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(HkexTradeOverviewPoster, self).__init__(item)
        self.mysql_reconnect_wait = 60
        self.dbname = get_hk_dbname(market = item['market'], direction = item['direction'])
        self.table = HkexCrawler.get_capital_table(self.dbname)
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()
        #self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        args = failure.value.args
        if failure.type in [OperationalError, InterfaceError]:
            # <class 'pymysql.err.OperationalError'> (1045, "Access denied for user 'username'@'localhost' (using password: YES)")
            # <class 'pymysql.err.OperationalError'> (2013, 'Lost connection to MySQL server during query ([Errno 110] Connection timed out)')
            # <class 'pymysql.err.OperationalError'> (2003, "Can't connect to MySQL server on '127.0.0.1' ([WinError 10061] 由于目标计算机积极拒绝，无法连接。)")
            # <class 'pymysql.err.InterfaceError'> (0, '')    # after crawl started: sudo service mysqld stop
            logger.info('MySQL: exception {} {}, Trying to recommit in {} sec'.format(failure.type, args, self.mysql_reconnect_wait)) 
            # https://twistedmatrix.com/documents/12.1.0/core/howto/time.html
            from twisted.internet import task
            from twisted.internet import reactor
            task.deferLater(reactor, self.mysql_reconnect_wait, self.async_store)
            return
        elif failure.type in [DataError, InternalError]:
            # <class 'pymysql.err.DataError'> (1264, "Out of range value for column 'position_id' at row 2")
            # <class 'pymysql.err.InternalError'> (1292, "Incorrect date value: '1977-06-31' for column 'release_day' at row 26")
            logger.warn('MySQL: {} {} exception from item {}'.format(failure.type, args, item))
            #self.store()
            return
        elif failure.type in [IntegrityError]:    
            # <class 'pymysql.err.IntegrityError'> (1048, "Column 'name' cannot be null") films 43894
            if failure.value.args[0] != 1062:
                logger.warn('MySQL: {} {} exception from some items'.format(failure.type, args))
                #self.store()
            return
        else:
            logger.error('MySQL: {} {} unhandled exception'.format(failure.type, args))
            return

    def async_store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            self.cursor.execute(insert_sql, params)
            self.connect.commit()
        except Exception as e:
            logger.debug(e)

class HkexTradeTopTenItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(HkexTradeTopTenItemPoster, self).__init__(item)
        self.mysql_reconnect_wait = 60
        self.dbname = get_hk_dbname(market = item['market'], direction = item['direction'])
        self.table = HkexCrawler.get_topten_table(self.dbname)
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()
        #self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        args = failure.value.args
        if failure.type in [OperationalError, InterfaceError]:
            # <class 'pymysql.err.OperationalError'> (1045, "Access denied for user 'username'@'localhost' (using password: YES)")
            # <class 'pymysql.err.OperationalError'> (2013, 'Lost connection to MySQL server during query ([Errno 110] Connection timed out)')
            # <class 'pymysql.err.OperationalError'> (2003, "Can't connect to MySQL server on '127.0.0.1' ([WinError 10061] 由于目标计算机积极拒绝，无法连接。)")
            # <class 'pymysql.err.InterfaceError'> (0, '')    # after crawl started: sudo service mysqld stop
            logger.info('MySQL: exception {} {}, Trying to recommit in {} sec'.format(failure.type, args, self.mysql_reconnect_wait)) 
            # https://twistedmatrix.com/documents/12.1.0/core/howto/time.html
            from twisted.internet import task
            from twisted.internet import reactor
            task.deferLater(reactor, self.mysql_reconnect_wait, self.store)
            return
        elif failure.type in [DataError, InternalError]:
            # <class 'pymysql.err.DataError'> (1264, "Out of range value for column 'position_id' at row 2")
            # <class 'pymysql.err.InternalError'> (1292, "Incorrect date value: '1977-06-31' for column 'release_day' at row 26")
            if failure.value.args[0] != 1062:
                logger.warn('MySQL: {} {} exception from item {}'.format(failure.type, args, item))
                #self.store()
            return
        elif failure.type in [IntegrityError]:    
            # <class 'pymysql.err.IntegrityError'> (1048, "Column 'name' cannot be null") films 43894
            logger.warn('MySQL: {} {} exception from some items'.format(failure.type, args))
            #self.store()
            return
        else:
            logger.error('MySQL: {} {} unhandled exception'.format(failure.type, args))
            return

    def async_store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            self.cursor.execute(insert_sql, params)
            self.connect.commit()
        except Exception as e:
            logger.debug(e)

class SPledgeSituationItemPoster(Poster):
    def __init__(self, item):
        self.item = item

class InvestorSituationItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        self.dbname = InvestorCrawler.get_dbname()
        self.table = InvestorCrawler.get_table_name()
        self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)
