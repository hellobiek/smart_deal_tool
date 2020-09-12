# -*- coding: utf-8 -*-
import const as ct
import pandas as pd
from base.clog import getLogger 
import datetime, re, xlrd, json
from scrapy import signals
from base.cdate import get_dates_array
from common import add_suffix
from datetime import datetime
from scrapy import FormRequest
from ccalendar import CCalendar
from dspider.items import MarginItem
from dspider.myspider import BasicSpider
STOCK_PAGE_URL = 'http://datacenter.eastmoney.com/api/data/get?type=RPTA_WEB_RZRQ_GGMX&sty=ALL&source=WEB&p={}&ps=500&st=date&sr=-1&var=uBrzlcAb&&filter=(date=%27{}%27)' 
class MarginSpider(BasicSpider):
    cur_count = 0
    total_count = 0
    cur_page = 1
    max_page = 0
    cal_client = CCalendar()
    logger = getLogger(__name__)
    name = 'marginSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 200,
            'dspider.pipelines.DspiderPipeline': 300,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_VALIDATION_MODELS': {
            MarginItem: 'dspider.validators.MarginModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MarginSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def item_scraped(self, item, response, spider):
        if item:
            self.cur_count += 1

    def spider_closed(self, spider, reason):
        message = 'scraped {} items, total {} items'.format(self.cur_count, self.total_count + 2)
        self.message = message
        if self.cur_count != self.total_count + 2:
            self.status = False
        else:
            self.status = True
        self.collect_spider_info()

    def start_requests(self):
        self.cur_count = 0
        matching_urls = ["http://datacenter.eastmoney.com/api/data/get?type=RPTA_WEB_RZRQ_LSSH&sty=ALL&source=WEB&st=dim_date&sr=-1&p=1&ps=50&var=tDckWaEJ&filter=(scdm=%22007%22)&rt=53262182",\
                         "http://datacenter.eastmoney.com/api/data/get?type=RPTA_WEB_RZRQ_LSSH&sty=ALL&source=WEB&st=dim_date&sr=-1&p=1&ps=5&var=JIsFtHAl&filter=(scdm=%22001%22)&rt=53262409"]
        mdate = datetime.now().strftime('%Y-%m-%d')
        if self.cal_client.is_trading_day(mdate):
            mdate = self.cal_client.pre_trading_day(mdate)
            self.cur_page = 1
            self.max_page = 1
            for url in matching_urls:
                yield FormRequest(url=url, callback=self.parse_market, meta={'date': mdate}, errback=self.errback_httpbin)
            url = STOCK_PAGE_URL.format(self.cur_page, mdate)
            yield FormRequest(url=url, callback=self.parse_stock, meta={'date': mdate}, errback=self.errback_httpbin)

    def parse_stock(self, response):
        try:
            if response.status != 200:
                self.logger.error('crawl page from url: {}, status: {} failed'.format(response.url, response.status))
                yield None
            info = json.loads(response.text.split('=')[1].split(';')[0])['result']
            data = info['data']
            df = pd.DataFrame(data)
            mdate = response.meta['date']
            if self.cur_page == 1:
                self.total_count = info['count']
                self.max_page = info['pages']
            for page in range(2, self.max_page + 1):
                self.cur_page += 1
                url = STOCK_PAGE_URL.format(page, mdate)
                yield FormRequest(url=url, callback=self.parse_stock, meta={'date': mdate}, errback=self.errback_httpbin)
            for unit in data:
                cur_date = unit['DATE'][0:10]
                if cur_date != mdate: continue
                item = MarginItem()
                item['date'] = mdate
                item['code'] = add_suffix(unit['SCODE'])
                item['rzye'] = float(self.value_of_none(unit['RZYE']))
                item['rzmre'] = float(self.value_of_none(unit['RZMRE']))
                item['rzche'] = float(self.value_of_none(unit['RZCHE']))
                item['rqye'] = float(self.value_of_none(unit['RQYE']))
                item['rqyl'] = float(self.value_of_none(unit['RQYL']))
                item['rqmcl'] = float(self.value_of_none(unit['RQMCL']))
                item['rqchl'] = float(self.value_of_none(unit['RQCHL']))
                item['rzrqye'] = float(self.value_of_none(unit['RZRQYE']))
                yield item
        except Exception as e:
            self.logger.error("get stock margin info exception:{}".format(e))

    def parse_market(self, response):
        try:
            if response.status != 200:
                self.logger.error('crawl page from url: {} status: {} failed'.format(response.url, response.status))
                yield None
            mdate = response.meta['date']
            data = json.loads(response.text.split('=')[1].split(';')[0])['result']['data']
            df = pd.DataFrame(data)
            df['DIM_DATE'] = df['DIM_DATE'].str[0:10]
            info = df.loc[df.DIM_DATE == mdate]
            if info.empty: yield None
            code = info['XOB_MARKET_0001'].values[0]
            item = MarginItem()
            item['date'] = mdate
            item['code'] = "SSE" if code.endswith('沪证') else "SZSE"
            item['rzye'] = float(self.value_of_none(info['RZYE'].values[0]))
            item['rzmre'] = float(self.value_of_none(info['RZMRE'].values[0]))
            item['rzche'] = float(self.value_of_none(info['RZCHE'].values[0]))
            item['rqye'] = float(self.value_of_none(info['RQYE'].values[0]))
            item['rqyl'] = float(self.value_of_none(info['RQYL'].values[0]))
            item['rqmcl'] = float(self.value_of_none(info['RQMCL'].values[0]))
            item['rqchl'] = float(self.value_of_none(info['RQCHL'].values[0]))
            item['rzrqye'] = float(self.value_of_none(info['RZRQYE'].values[0]))
            yield item
        except Exception as e:
            self.logger.error("get market margin info exception:{}".format(e))
