# -*- coding: utf-8 -*-
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from spiders.investorSituationSpider import InvestorsituationspiderSpider 

def main():
    runner = CrawlerRunner(get_project_settings())
    d = runner.crawl(InvestorsituationspiderSpider())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == '__main__':
    main()
