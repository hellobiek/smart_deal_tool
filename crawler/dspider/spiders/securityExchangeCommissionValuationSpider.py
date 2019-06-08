# -*- coding: utf-8 -*-
import datetime
import const as ct
from datetime import datetime
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import MyDownloadItem
class SecurityExchangeCommissionValuationSpider(BasicSpider):
    name = 'securityExchangeCommissionValuationSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'FILES_STORE': ct.SECURITY_EXCHANGE_COMMISSION_INDUSTRY_VALUATION_ZIP_PATH,
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'SPIDERMON_EXPECTED_FINISH_REASONS': ct.SPIDERMON_EXPECTED_FINISH_REASONS,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'dspider.pipelines.PlateValuationDownloadPipeline': 100,
            'dspider.pipelines.SecurityExchangeCommissionValuationPipeline': 200,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    allowed_domains = ['115.29.210.20']
    start_url = 'http://115.29.210.20/syl/'
    def start_requests(self):
        mformat = '%Y%m%d.zip'
        end_date = datetime.now().strftime(mformat)
        start_date = self.get_nday_ago(end_date, 5000, dformat = mformat)
        while start_date < end_date:
            furl =  self.start_url + start_date
            yield FormRequest(url = furl, method = 'GET', callback = self.parse, errback=self.errback_httpbin)
            start_date = self.get_tomorrow_date(sdate = start_date, dformat = mformat)

    def parse(self, response):
        try:
            tmpContDis = response.headers['Content-Disposition']
            if tmpContDis is not None:
                fname = tmpContDis.decode().split('=')[1]
                yield MyDownloadItem(file_urls = [response.url], file_name = fname)
        except Exception as e:
            print(e)
