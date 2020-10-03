# -*- coding: utf-8 -*-
import json, random
import const as ct
import pandas as pd
import urllib.parse
from scrapy import signals
from scrapy import FormRequest
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from dspider.items import BlockTradingItem
from dspider.myspider import BasicSpider
class BlockTradingSpider(BasicSpider):
    name = 'blockTradingSpider'
    cur_count = 0
    sse_cur_page = 1
    sse_max_page = 5
    sse_total_count = 0
    szse_total_count = 0
    cal_client = CCalendar()
    logger = getLogger(__name__)
    headers = {'Referer': 'http://www.sse.com.cn/disclosure/diclosure/block/deal/'}
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'USER_AGENTS': ct.USER_AGENTS,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'dspider.user_agent.RandomUserAgent': 200,
            'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None
        },
        'ITEM_PIPELINES': {
            'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 200,
            'dspider.pipelines.DspiderPipeline': 300,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_VALIDATION_MODELS': {
            BlockTradingItem: 'dspider.validators.BlockTradingModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BlockTradingSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(spider.item_error, signal=signals.item_error)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_error(self, failure, response, spider):
        self.logger.error("wrong url:{}, failure:{}".format(response.url, failure.value))

    def item_error(self, item, response, spider, failure):
        self.logger.error("wrong item date:{}, code:{}, failure:{}".format(item['date'], item['code'], failure.value))

    def item_scraped(self, item, response, spider):
        if item:
            self.cur_count += 1

    def item_dropped(self, item, spider, exception):
        self.logger.error("exception date:{}, code:{}, failure:{}".format(item['date'], item['code'], exception))

    def spider_closed(self, spider, reason):
        message = 'scraped {} items, total {} items'.format(self.cur_count, self.szse_total_count + self.sse_total_count)
        self.message = message
        if self.cur_count != self.szse_total_count + self.sse_total_count:
            self.logger.error(messsage)
            self.status = False
        else:
            self.status = True
        self.collect_spider_info()

    def sse_url(self, page, mdate, stock_id = '', pagesize = 300, max_page = 5):
        data = {'isPagination':'true',
                'sqlId': 'COMMON_SSE_XXPL_JYXXPL_DZJYXX_L_1',
                'stockId': stock_id,
                'startDate': mdate,
                'endDate': mdate,
                'pageHelp.pageSize': str(pagesize),
                'pageHelp.pageNo': str(page),
                'pageHelp.beginPage': str(page),
                'pageHelp.cacheSize': '1',
                'pageHelp.endPage': str(max_page)}
        url = 'http://query.sse.com.cn/commonQuery.do?'+ urllib.parse.urlencode(data)
        return url

    def szse_url(self, mdate):
        url = "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1932_dzjyzqjy&TABKEY=tab2&txtStart={}&txtEnd={}&random={}".format(mdate, mdate, random.random())
        return url

    def start_requests(self):
        self.cur_count = 0
        self.sse_cur_page = 1
        self.sse_max_page = 5
        self.sse_total_count = 0
        self.szse_total_count = 0
        mdate = datetime.now().strftime('%Y-%m-%d')
        if self.cal_client.is_trading_day(mdate):
            sse_url = self.sse_url(self.sse_cur_page, mdatej)
            yield FormRequest(url=sse_url, headers = self.headers, callback=self.parse_sse, meta={'date': mdate}, errback=self.errback_httpbin)

    def parse_szse(self, mdate):
        url = self.szse_url(mdate)
        df = pd.read_excel(url, header = 0, names = ['date', 'code', 'name', 'price', 'volume', 'amount', 'branch_buy', 'branch_sell'])
        df['code'] = df['code'].map(lambda x : str(x).zfill(6))
        return df

    def parse_sse(self, response):
        try:
            if response.status != 200:
                self.logger.error('crawl page from url: {}, status: {} failed'.format(response.url, response.status))
                yield None
            mdate = response.meta['date']
            info = json.loads(response.text)['pageHelp']
            if self.sse_cur_page == 1:
                self.sse_total_count = int(info["total"])
                self.sse_max_page = int(info["pageCount"])
                df = self.parse_szse(mdate)
                self.szse_total_count = len(df)
                for index, row in df.iterrows():
                    item = BlockTradingItem()
                    item['uid'] = "szse_{}".format(index + 1)
                    item['date'] = row['date']
                    item['code'] = row['code']
                    item['name'] = row['name']
                    item['price'] = float(row['price'])
                    item['volume'] = float(row['volume'])
                    item['amount'] = float(row['amount'])
                    item['branch_buy'] = row['branch_buy']
                    item['branch_sell'] = row['branch_sell']
                    yield item
            for data in info["data"]:
                item = BlockTradingItem()
                item['uid'] = "sse_{}".format(data['NUM'])
                item['date'] = data['tradedate']
                item['code'] = data['stockid']
                item['name'] = data['abbrname']
                item['price'] = float(data['tradeprice'])
                item['volume'] = float(data['tradeqty'])
                item['amount'] = float(data['tradeamount'])
                item['branch_buy'] = data['branchbuy']
                item['branch_sell'] = data['branchsell']
                yield item
            if self.sse_cur_page < self.sse_max_page:
                self.sse_cur_page += 1
                sse_url = self.sse_url(self.sse_cur_page, mdate, max_page = self.sse_max_page)
                yield FormRequest(url=sse_url, headers = self.headers, callback=self.parse_sse, meta={'date': mdate}, errback=self.errback_httpbin)
        except Exception as e:
            self.logger.error("get stock block trading info exception:{}".format(e))
