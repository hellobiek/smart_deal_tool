# -*- coding: utf-8 -*-
import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from spiders.investorSituationSpider import InvestorSituationSpider 

def main():
    runner = CrawlerRunner(get_project_settings())
    d = runner.crawl(InvestorSituationSpider())
    import pdb
    pdb.set_trace()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == '__main__':
    main()
