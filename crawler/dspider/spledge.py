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
from dspider.spiders.spledgeSituationSpider import SPledgeSituationSpider
class SPledgeCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbname = self.get_dbname()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("init pledge database failed")

    @staticmethod
    def get_dbname():
        return "spledge"

    def create_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(10) not null,\
                                             name varchar(50),\
                                             pledge_counts int,\
                                             unlimited_pledge_stocks float,\
                                             limited_pledge_stocks float,\
                                             total_stocks float,\
                                             pledge_ratio float,\
                                             PRIMARY KEY(date, code))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def run(self):
        settings = get_project_settings()
        myrunner = CrawlerRunner(settings)
        myrunner.crawl(SPledgeSituationSpider)
        d = myrunner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run() #the script will block here until the crawling is finished

if __name__ == '__main__':
    spc = SPledgeCrawler()
    spc.run()
