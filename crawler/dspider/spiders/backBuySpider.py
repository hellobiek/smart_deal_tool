# -*- coding: utf-8 -*-
import json
import const as ct
import pandas as pd
import urllib.parse
from scrapy import signals
from datetime import datetime
from scrapy import FormRequest
from base.clog import getLogger 
from base.cdate import str_to_datetime, datetime_to_str
from dspider.myspider import BasicSpider
'''
dim_scode: 代码
securityshortname: 股票简称
newprice: 最新股价
cprice: 公告前一日收盘价
repurpricelower: 计划回购价格下限(元)
repurpricecap: 计划回购价格上限(元)	
repurnumlower: 计划回购数量下限(股)	
repurnumcap: 计划回购数量上限(股)
repuramountlower: 计划回购金额下限(元)
repuramountlimit: 计划回购金额上限(元)	
dim_date: 回购公告日
sharetype: 回购股份类型
ltszxx: 占公告前一日流通市值比例下限(%)
ltszsx: 占公告前一日流通市值比例上限(%)
zszxx: 占公告前一日总股本比例下限(%)
zszsx: 占公告前一日总股本比例上限(%)
shmrsltnoticedate: 股东大会决议公告日期	
repurobjective: 回购目的
remark: 特别提示
repurprogress: [001(董事会预案), 002(股东大会通过), 003(股东大会否决), 004(实施中), 005(停止实施), 006(完成实施)]
updatedate: 状态更新日期
repurstartdate: 回购起始日期
repurenddate: 回购截止日期
repurpricelower1: 已回购股份价格下限(元)
repurpricecap1: 已回购股份价格上限(元)
repurnum: 已回购股份数量(股)
repuramount: 已回购金额(元)
repuradvancedate: 提前完成日期
'''
class BackBuySpider(BasicSpider):
    name = 'backBuySpider'
    logger = getLogger(__name__)
    cur_page = 1
    max_page = 5
    cur_count = 0
    total_count = 0
    data = list()
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
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BackBuySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_error(self, failure, response, spider):
        self.logger.error("wrong url:{}, failure:{}".format(response.url, failure.value))

    def save_data(self, df, mdate):
        try:
            df['date'] = mdate
            df= df[['date', 'dim_scode', 'securityshortname', 'newprice', 'cprice', 'repurpricelower', 'repurpricecap',\
                    'repurnumlower', 'repurnumcap', 'repuramountlower', 'repuramountlimit', 'dim_date', 'sharetype',\
                    'ltszxx', 'ltszsx', 'zszxx', 'zszsx', 'shmrsltnoticedate', 'repurobjective', 'remark', 'repurprogress',\
                    'updatedate', 'repurstartdate', 'repurenddate', 'repurpricelower1', 'repurpricecap1', 'repurnum',\
                    'repuramount', 'repuradvancedate']]
            df = df.rename(columns = {"dim_scode": "code", "securityshortname": "name"})
            df.fillna('', inplace = True)
            for key in ['dim_date', 'shmrsltnoticedate', 'updatedate', 'repurstartdate', 'repurenddate', 'repuradvancedate']:
                df[key] = df[key].apply(lambda x:str_to_datetime(x, "%Y/%m/%d"))
                df[key] = df[key].apply(lambda x:datetime_to_str(x, "%Y-%m-%d"))
            filepath = os.path.join(ct.STOCK_BACKBUY_FILE_PATH, "{}.csv".format(mdate))
            df.to_csv(filepath, index=False, mode="w", encoding='utf8')
            return True
        except Exception as e:
            self.logger.error("store back buy info exception:{}".format(e))
            return False

    def spider_closed(self, spider, reason):
        df = pd.DataFrame(self.data)
        df.drop_duplicates(subset=['dim_scode','dim_date'])
        mdate = datetime.now().strftime('%Y-%m-%d')
        if len(df) == self.total_count: 
            if self.save_data(df, mdate):
                message = 'scraped backbuy info succeed for {}'.format(mdate)
                self.status = True
                self.message = message
            else:
                message = 'store backbuy info failed for {}'.format(mdate)
                self.status = False
                self.message = message
        else:
            message = 'scraped backbuy info failed for {}, actual count:{}, expected count:{}'.format(mdate, len(df), self.total_count)
            self.status = False
            self.logger.error(messsage)
        self.collect_spider_info()

    def east_url(self, page, pagesize = 300):
        data = {'type': 'RPTA_WEB_GETHGLIST',
                'sty': 'ALL',
                'source': 'WEB',
                'p': str(page),
                'ps': str(pagesize),
                'st': 'dim_date',
                'sr': str(-1)}
        return "http://datacenter.eastmoney.com/api/data/get?" + urllib.parse.urlencode(data)

    def start_requests(self):
        self.cur_page = 1
        self.max_page = 5
        self.total_count = 0
        url = self.east_url(self.cur_page)
        yield FormRequest(url=url, callback=self.parse, errback=self.errback_httpbin)

    def parse(self, response):
        try:
            if response.status != 200:
                self.logger.error('crawl page from url: {}, status: {} failed'.format(response.url, response.status))
                yield None
            info = json.loads(response.text)
            data = info['result']['data']
            if self.cur_page == 1:
                self.data = data
                self.max_page = info['result']['pages']
                self.total_count = info['result']['count']
            else:
                self.data.extend(data)
            if self.cur_page < self.max_page:
                self.cur_page += 1
                url = self.east_url(self.cur_page)
                yield FormRequest(url=url, callback=self.parse, errback=self.errback_httpbin)
        except Exception as e:
            self.logger.error("get stock back buy info exception:{}".format(e))
