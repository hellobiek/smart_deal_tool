# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(abspath(__file__)))
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import json
import pymysql
import pymysql.cursors
import const as ct
from base.clog import getLogger
from twisted.enterprise import adbapi
from cmysql import CMySQL
from hkex import HkexCrawler
from common import create_redis_obj
from investor import InvestorCrawler
from investor import MonthInvestorCrawler
from plate_valuation import PlateValuationCrawler
from stock_limit_crawler import StockLimitCrawler
from china_treasury_rate import ChinaTreasuryRateCrawler
from china_security_industry_valuation import ChinaSecurityIndustryValuationCrawler
from pymysql.err import OperationalError, InterfaceError, DataError, InternalError, IntegrityError
logger = getLogger(__name__)
class Poster(object):
    def __init__(self, item):
        self.item = item
        self.table = ''

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql(self.table)
        cursor.execute(insert_sql, params)

    def check(self):
        if not ct.SPIDERMON_VALIDATION_ERRORS_FIELD in self.item: return True
        errors = self.item[ct.SPIDERMON_VALIDATION_ERRORS_FIELD]
        logger.error("{} check failed:{}, item:{}" % (self.__class__, json.dumps(errors, indent=4), self.item))
        return False

    def store(self):
        raise NotImplementedError()

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
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)

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
            return
        elif failure.type in [IntegrityError]:    
            # <class 'pymysql.err.IntegrityError'> (1048, "Column 'name' cannot be null") films 43894
            if failure.value.args[0] != 1062:
                logger.warn('MySQL: {} {} exception from some items'.format(failure.type, args))
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
            if not self.mysql_client.exec_sql(insert_sql, params, retry_times = 3):
                logger.error("store failed for :{}".format(self.item))
        except Exception as e:
            logger.debug(e)

class HkexTradeTopTenItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(HkexTradeTopTenItemPoster, self).__init__(item)
        self.mysql_reconnect_wait = 60
        self.dbname = get_hk_dbname(market = item['market'], direction = item['direction'])
        self.table = HkexCrawler.get_topten_table(self.dbname)
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)

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
            return
        elif failure.type in [IntegrityError]:    
            # <class 'pymysql.err.IntegrityError'> (1048, "Column 'name' cannot be null") films 43894
            logger.warn('MySQL: {} {} exception from some items'.format(failure.type, args))
            return
        else:
            logger.error('MySQL: {} {} unhandled exception'.format(failure.type, args))
            return

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            if not self.mysql_client.exec_sql(insert_sql, params, retry_times = 3):
                logger.error("store failed for :{}".format(self.item))
        except Exception as e:
            logger.debug(e)

class MyDownloadItemPoster(Poster):
    def __init__(self, item):
        super(MyDownloadItemPoster, self).__init__(item)

class SPledgeSituationItemPoster(Poster):
    def __init__(self, item):
        super(SPledgeSituationItemPoster, self).__init__(item)

class ChinaSecurityIndustryValuationPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO, redis_host = None):
        super(ChinaSecurityIndustryValuationPoster, self).__init__(item)
        self.dbname = ChinaSecurityIndustryValuationCrawler.get_dbname()
        self.table = ChinaSecurityIndustryValuationCrawler.get_tablename()
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            self.cursor.execute(insert_sql, params)
            self.connect.commit()
        except Exception as e:
            logger.debug(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

class ChinaTreasuryRateItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO, redis_host = None):
        super(ChinaTreasuryRateItemPoster, self).__init__(item)
        self.dbname = ChinaTreasuryRateCrawler.get_dbname()
        self.table = ChinaTreasuryRateCrawler.get_tablename()
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            self.cursor.execute(insert_sql, params)
            self.connect.commit()
        except Exception as e:
            logger.debug(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

class PlateValuationPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(PlateValuationPoster, self).__init__(item)
        self.dbname = PlateValuationCrawler.get_dbname()
        self.table = PlateValuationCrawler.get_tablename()
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            self.cursor.execute(insert_sql, params)
            self.connect.commit()
        except Exception as e:
            logger.error(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

class InvestorSituationItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(InvestorSituationItemPoster, self).__init__(item)
        self.dbname = InvestorCrawler.get_dbname()
        self.table = InvestorCrawler.get_table_name()
        self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

class MonthInvestorSituationItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(MonthInvestorSituationItemPoster, self).__init__(item)
        self.dbname = MonthInvestorCrawler.get_dbname()
        self.table = MonthInvestorCrawler.get_table_name()
        self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

class StockLimitItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO, redis_host = None):
        super(StockLimitItemPoster, self).__init__(item)
        self.dbname = StockLimitCrawler.get_dbname()
        self.table = StockLimitCrawler.get_tablename()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)

    def store(self):
        try:
            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            if not self.mysql_client.exec_sql(insert_sql, params, retry_times = 3):
                logger.error("store failed for :{}".format(self.item))
        except Exception as e:
            logger.debug(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

class MarginItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO, redis_host = None):
        super(MarginItemPoster, self).__init__(item)
        self.dbname = "margin"
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        self.connect = pymysql.connect(host=dbinfo['host'], port=dbinfo['port'], db=self.dbname, user=dbinfo['user'], passwd=dbinfo['password'], charset=ct.UTF8)
        self.cursor = self.connect.cursor()

    def get_dbname(self):
        return self.dbname

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "margin_day_{}_{}".format(cdates[0], (int(cdates[1])-1)//3 + 1)

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             rzye float,\
                                             rzmre float,\
                                             rzche float,\
                                             rqye float,\
                                             rqyl float,\
                                             rqmcl float,\
                                             rqchl float,\
                                             rzrqye float,\
                                             PRIMARY KEY (date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return self.redis.sismember(self.dbname, table_name)
        return False

    def store(self):
        try:
            mdate = self.item['date']
            code = self.item['code']
            self.table = self.get_table_name(mdate)
            if not self.is_table_exists(self.table):
                if not self.create_table(self.table):
                    logger.error("create margin table failed")
                    return
                self.redis.sadd(self.dbname, self.table)

            if self.is_date_exists(self.table, "{}{}".format(mdate, code)):
                logger.debug("existed table:{}, date:{}".format(self.table, mdate))
                return

            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            if self.mysql_client.exec_sql(insert_sql, params, retry_times = 3):
                self.redis.sadd(self.table, "{}{}".format(mdate, code))
            else:
                logger.error("store failed for :{}".format(self.item))
        except Exception as e:
            logger.error(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())

class BlockTradingItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO, redis_host = None):
        super(BlockTradingItemPoster, self).__init__(item)
        self.dbname = "block_trading"
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                              uid varchar(10) not null,\
                                             code varchar(10) not null,\
                                             name varchar(20) not null,\
                                             price float,\
                                             volume float,\
                                             amount float,\
                                             branch_buy varchar(200),\
                                             branch_sell varchar(200),\
                                             PRIMARY KEY (date, uid, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get_dbname(self):
        return self.dbname

    def get_table_name(self, cdate):
        cdates = cdate.split('-')
        return "block_trading_day_{}_{}".format(cdates[0], (int(cdates[1])-1)//3 + 1)

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return self.redis.sismember(self.dbname, table_name)
        return False

    def store(self):
        try:
            mdate = self.item['date']
            code = self.item['code']
            uid = self.item['uid']
            self.table = self.get_table_name(mdate)
            if not self.is_table_exists(self.table):
                if not self.create_table(self.table):
                    logger.error("create tick table failed")
                    return
                self.redis.sadd(self.dbname, self.table)

            if self.is_date_exists(self.table, "{}{}{}".format(mdate, uid, code)):
                logger.debug("existed table:{}, mdate:{}, uid:{}, code:{}".format(self.table, mdate, uid, code))
                return

            insert_sql, params = self.item.get_insert_sql(self.table)
            if insert_sql == None and params == None: return
            if self.mysql_client.exec_sql(insert_sql, params, retry_times = 3):
                self.redis.sadd(self.table, "{}{}{}".format(mdate, uid, code))
            else:
                logger.error("store failed for :{}".format(self.item))
        except Exception as e:
            logger.error(e)

    def on_error(self, failure):
        if not (failure.type == IntegrityError and failure.value.args[0] == 1062):
            logger.error(failure.type, failure.value, failure.getTraceback())
