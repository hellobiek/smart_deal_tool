#coding=utf-8
import sys
import traceback
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from dspider.hkex import HkexCrawler
from dspider.spledge import SPledgeCrawler
from dspider.investor import InvestorCrawler
from dspider.plate_valuation import PlateValuationCrawler
from dspider.investor import MonthInvestorCrawler
from scrapy.utils.project import get_project_settings
from dspider.spiders.hkexSituationSpider import HkexSpider
from dspider.spiders.plateValuation import PlateValuationSpider
from dspider.spiders.spledgeSituationSpider import SPledgeSituationSpider
from dspider.spiders.investorSituationSpider import InvestorSituationSpider
from dspider.spiders.investorMonthSituationSpider import MonthInvestorSituationSpider

def init():
    #HkexCrawler()
    #SPledgeCrawler()
    #InvestorCrawler()
    #MonthInvestorCrawler()
    PlateValuationCrawler()

def weekly_spider():
    try:
        init()
        settings = get_project_settings()
        myrunner = CrawlerRunner(settings)
        #myrunner.crawl(HkexSpider)
        #myrunner.crawl(SPledgeSituationSpider)
        #myrunner.crawl(InvestorSituationSpider)
        #myrunner.crawl(MonthInvestorSituationSpider)
        myrunner.crawl(PlateValuationSpider)
        d = myrunner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run() #the script will block here until the crawling is finished
    except Exception as e:
        traceback.print_exc()
        print(e)

weekly_spider()
