#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from log import getLogger
from cmysql import CMySQL
from common import create_redis_obj
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from dspider.spiders.investorSituationSpider import InvestorSituationSpider
class InvestorCrawler(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.logger = getLogger(__name__)
        self.dbname = self.get_dbname()
        self.table = self.get_table_name()
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = CMySQL(dbinfo, self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname): raise Exception("init margin database failed")
        if not self.create_table(): raise Exception("init margin table failed")

    @staticmethod
    def get_dbname():
        return "stock"

    @staticmethod
    def get_table_name():
        return "investor"

    def create_table(self):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             new_investor float,\
                                             final_investor float,\
                                             new_natural_person float,\
                                             new_non_natural_person float,\
                                             final_natural_person float,\
                                             final_non_natural_person float,\
                                             PRIMARY KEY(date))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    def run(self):
        settings = get_project_settings()
        myrunner = CrawlerRunner(settings)
        myrunner.crawl(InvestorSituationSpider)
        d = myrunner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run() # the script will block here until the crawling is finished

if __name__ == '__main__':
    ic = InvestorCrawler()
    ic.run()
