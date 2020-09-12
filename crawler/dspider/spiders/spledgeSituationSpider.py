# -*- coding: utf-8 -*-
import calendar
import datetime
import const as ct
from pathlib import Path
from scrapy import signals
from datetime import datetime
from base.clog import getLogger 
from base.cdate import get_next_date, get_pre_date, is_some_day
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import SPledgeSituationItem
class SPledgeSituationSpider(BasicSpider):
    name = 'spledgeSituationSpider'
    logger = getLogger(__name__)
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'FILES_STORE': ct.PLEDGE_FILE_DIR,
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
        start_date = self.get_nday_ago(end_date, 30, dformat = '%Y.%m.%d')
        while start_date <= end_date:
            formdata['queryDate'] = start_date
            yield FormRequest(url = self.start_urls[0], method = 'GET', formdata = formdata, callback = self.parse, errback=self.errback_httpbin)
            start_date = get_next_date(sdate = start_date, target_day = calendar.SATURDAY)

    def get_file_name(self, edate):
        if is_some_day(edate, calendar.SATURDAY, dformat = '%Y-%m-%d'):
            sdate = get_pre_date(sdate = edate, target_day = calendar.SUNDAY, dformat = '%Y-%m-%d')
        else:
            edate = get_pre_date(sdate = edate, target_day = calendar.SATURDAY, dformat = '%Y-%m-%d')
            sdate = get_pre_date(sdate = edate, target_day = calendar.SUNDAY, dformat = '%Y-%m-%d')
        sdate = sdate.replace('-', '')
        edate = edate.replace('-', '')
        return "{}_{}.xls".format(sdate, edate) 

    def parse(self, response):
        fname = response.headers['Content-Disposition'].decode().split('=')[1]
        yield SPledgeSituationItem(file_urls = [response.url], file_name = fname)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SPledgeSituationSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        mdate = datetime.now().strftime('%Y-%m-%d')
        file_name = self.get_file_name(mdate)
        file_path = Path(ct.PLEDGE_FILE_DIR)/"{}".format(file_name)
        if file_path.exists():
            message = "download pledge info {} at {} succeed".format(file_path, mdate)
            self.status = True
        else:
            message = "download pledge info {} at {} failed".format(file_path, mdate)
            self.status = False
        self.message = message
        self.collect_spider_info()
