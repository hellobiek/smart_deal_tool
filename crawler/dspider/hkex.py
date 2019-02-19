#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cmysql import CMySQL
from common import create_redis_obj
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from dspider.spiders.hkexSituationSpider import HkexSpider
class HkexCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        for (market_from, market_to) in [(ct.SH_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL), (ct.HK_MARKET_SYMBOL, ct.SH_MARKET_SYMBOL), (ct.SZ_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL), (ct.HK_MARKET_SYMBOL, ct.SZ_MARKET_SYMBOL)]:
            dbname = self.get_dbname(market_from, market_to) 
            self.mysql_client = CMySQL(dbinfo, dbname)
            if not self.mysql_client.create_db(dbname): raise Exception("init hkex crawler database failed")
            if not self.create_topten_table(dbname): raise Exception("init hkex crawler topten table failed")
            if not self.create_capital_table(dbname): raise Exception("init hkex crawler capital table failed failed")

    @staticmethod
    def get_dbname(market_from, market_to):
        return "%s2%s" % (market_from, market_to)

    @staticmethod
    def get_topten_table(dbname):
        return "%s_topten" % dbname

    @staticmethod
    def get_capital_table(dbname):
        return "%s_capital_overview" % dbname

    def create_topten_table(self, dbname):
        table = self.get_topten_table(dbname)
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             name varchar(50),\
                                             rank int,\
                                             total_turnover float,\
                                             buy_turnover float,\
                                             sell_turnover float,\
                                             PRIMARY KEY(date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)


    def create_capital_table(self, dbname):
        table = self.get_capital_table(dbname)
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             total_turnover float,\
                                             buy_turnover float,\
                                             sell_turnover float,\
                                             total_trade_count long,\
                                             buy_trade_count long,\
                                             sell_trade_count long,\
                                             PRIMARY KEY(date))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def run(self):
        settings = get_project_settings()
        myrunner = CrawlerRunner(settings)
        myrunner.crawl(HkexSpider)
        d = myrunner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run() #the script will block here until the crawling is finished

if __name__ == '__main__':
    hc = HkexCrawler()
    hc.run()
