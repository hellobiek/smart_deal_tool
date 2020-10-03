# -*- coding: utf-8 -*-
import os
import datetime
import const as ct
from pathlib import Path
from scrapy import signals
from datetime import datetime
from scrapy import FormRequest
from base.clog import getLogger
from dspider.myspider import BasicSpider
from dspider.items import MyDownloadItem, ChinaSecurityIndustryValuationItem
class ChinaSecurityIndustryValuationSpider(BasicSpider):
    #name = 'chinaSecurityIndustryValuationSpider'
    name = 'cSIValuationSpider'
    file_name = ''
    logger = getLogger(__name__)
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'FILES_STORE': ct.CHINA_SECURITY_INDUSTRY_VALUATION_PATH,
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'SPIDERMON_EXPECTED_FINISH_REASONS': ct.SPIDERMON_EXPECTED_FINISH_REASONS,
        'SPIDERMON_VALIDATION_MODELS': {
            ChinaSecurityIndustryValuationItem: 'dspider.validators.PlateValuationModel',
        },
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'dspider.pipelines.PlateValuationDownloadPipeline': 100,
            'dspider.pipelines.ChinaSecurityIndustryValuationHandlePipeline': 200,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    allowed_domains = ['47.97.204.47']
    start_url = 'http://47.97.204.47/syl/'
    def start_requests(self):
        mformat = 'csi%Y%m%d.zip'
        end_date = datetime.now().strftime(mformat)
        self.file_name = end_date
        start_date = self.get_nday_ago(end_date, 10, dformat = mformat)
        while start_date <= end_date:
            furl =  self.start_url + start_date
            yield FormRequest(url = furl, method = 'GET', callback = self.parse, errback=self.errback_httpbin)
            start_date = self.get_tomorrow_date(sdate = start_date, dformat = mformat)

    def parse(self, response):
        try:
            if response.status == 200:
                fname = os.path.basename(response.url)
                yield MyDownloadItem(file_urls = [response.url], file_name = fname)
            else:
                self.logger.error("get china security industry valuation failed url:{} status:{}".format(response.url, response.status))
        except Exception as e:
            self.logger.error("get china security industry valuation exception:{}".format(e))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ChinaSecurityIndustryValuationSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        mdate = datetime.now().strftime('%Y-%m-%d')
        file_path = Path(ct.CHINA_SECURITY_INDUSTRY_VALUATION_PATH)/"{}".format(self.file_name)
        if file_path.exists():
            message = "download china security industry valuation {} at {} succeed".format(file_path, mdate)
            self.status = True
        else:
            message = "download china security industry valuation {} at {} failed".format(file_path, mdate)
            self.status = False
        self.message = message
        self.collect_spider_info()
