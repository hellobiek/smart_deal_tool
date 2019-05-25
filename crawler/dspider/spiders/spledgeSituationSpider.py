# -*- coding: utf-8 -*-
import calendar
import datetime
import const as ct
from datetime import datetime
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import SPledgeSituationItem
class SPledgeSituationSpider(BasicSpider):
    name = 'spledgeSituationSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'FILES_STORE': '/data/tdx/history/weeks/pledge',
        'SPIDERMON_EXPECTED_FINISH_REASONS': ct.SPIDERMON_EXPECTED_FINISH_REASONS,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'dspider.pipelines.SPledgePipline': 200,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    allowed_domains = ['www.chinaclear.cn']
    start_urls = ['http://www.chinaclear.cn/cms-rank/downloadFile']
    def start_requests(self):
        formdata = dict()
        formdata['queryDate'] = ''
        formdata['type'] = 'proportion'
        end_date = datetime.now().strftime('%Y.%m.%d')
        start_date = self.get_nday_ago(end_date, 60, dformat = '%Y.%m.%d')
        while start_date < end_date:
            start_date = self.get_next_date(sdate = start_date, target_day = calendar.SATURDAY)
            if start_date > end_date: continue
            formdata['queryDate'] = start_date
            yield FormRequest(url = self.start_urls[0], method = 'GET', formdata = formdata, callback = self.parse)

    def parse(self, response):
        fname = response.headers['Content-Disposition'].decode().split('=')[1]
        yield SPledgeSituationItem(file_urls = [response.url], file_name = fname)
