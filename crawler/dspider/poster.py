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
from pymysql.err import IntegrityError
class Poster(object):
    def __init__(self, item):
        self.item = item
        self.table = ''
        self.logger = getLogger(__name__)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql(self.table)
        cursor.execute(insert_sql, params)

    def store(self):
        print(self.item.convert())

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
        self.dbname = get_hk_dbname(market = item['market'], direction = item['direction'])
        self.table = HkexCrawler.get_capital_table(self.dbname)
        self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        self.logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

class HkexTradeTopTenItemPoster(Poster):
    def __init__(self, item, dbinfo = ct.DB_INFO):
        super(HkexTradeTopTenItemPoster, self).__init__(item)
        self.dbname = get_hk_dbname(market = item['market'], direction = item['direction'])
        self.table = HkexCrawler.get_topten_table(self.dbname)
        self.dbpool = adbapi.ConnectionPool("pymysql", host = dbinfo['host'], db = self.dbname, user = dbinfo['user'], password = dbinfo['password'], charset = "utf8", cursorclass = pymysql.cursors.DictCursor, use_unicode = True)

    def on_error(self, failure):
        self.logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)

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
            self.logger.error(failure.type, failure.value, failure.getTraceback())

    def store(self):
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        query.addErrback(self.on_error)
