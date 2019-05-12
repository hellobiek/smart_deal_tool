#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from dspider.hkex import HkexCrawler
from dspider.spledge import SPledgeCrawler
from dspider.investor import InvestorCrawler
from scrapy.utils.project import get_project_settings
from dspider.spiders.hkexSituationSpider import HkexSpider
from dspider.spiders.spledgeSituationSpider import SPledgeSituationSpider
from dspider.spiders.investorSituationSpider import InvestorSituationSpider
def init():
    #HkexCrawler()
    #SPledgeCrawler()
    InvestorCrawler()

def weekly_spider():
    init()
    settings = get_project_settings()
    myrunner = CrawlerRunner(settings)
    #myrunner.crawl(HkexSpider)
    #myrunner.crawl(SPledgeSituationSpider)
    myrunner.crawl(InvestorSituationSpider)
    d = myrunner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run() #the script will block here until the crawling is finished

weekly_spider()
