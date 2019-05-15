# -*- coding: utf-8 -*-
import calendar
import datetime
import const as ct
from datetime import datetime
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import MyDownloadItem
class PlatePERatioSpider(BasicSpider):
    name = 'platePERatioSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'FILES_STORE': '/data/crawler/plate_pe_ratio',
        'SPIDERMON_EXPECTED_FINISH_REASONS': ct.SPIDERMON_EXPECTED_FINISH_REASONS,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'dspider.pipelines.PlatePERatioPipline': 200,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    allowed_domains = ['115.29.210.20']
    start_url = 'http://115.29.210.20/syl/'
    def start_requests(self):
        mformat = 'bk%Y%m%d.zip'
        end_date = datetime.now().strftime(mformat)
        start_date = self.get_nday_ago(end_date, 20, dformat = mformat)
        start_date = 'bk20130101.zip'
        while start_date < end_date:
            furl =  self.start_url + start_date
            yield FormRequest(url = furl, method = 'GET', callback = self.parse)
            start_date = self.get_tomorrow_date(sdate = start_date, dformat = mformat)

    def parse(self, response):
        tmpContDis = response.headers['Content-Disposition']
        if tmpContDis is not None:
            fname = tmpContDis.decode().split('=')[1]
            yield MyDownloadItem(file_urls = [response.url], file_name = fname)
