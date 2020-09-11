# -*- coding: utf-8 -*-
import os, json
import datetime
import const as ct
import pandas as pd
from scrapy import signals
from datetime import datetime
from scrapy import FormRequest
from base.clog import getLogger
from dspider.myspider import BasicSpider
from base.cdate import one_report_date_list
class FinDisclosureSpider(BasicSpider):
    logger = getLogger(__name__)
    name = 'finDisclosureSpider'
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
    allowed_domains = ['www.cninfo.com.cn']
    start_url = 'http://www.cninfo.com.cn/new/information/getPrbookInfo'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FinDisclosureSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        self.status = False
        self.message = ""
        params = {"sectionTime": "", "firstTime": "", "lastTime": "", "market": "szsh" , "stockCode": "", "orderClos": "", "isDesc": "", "pagesize": "10000", "pagenum": "1",}
        date_list = one_report_date_list(datetime.now().strftime('%Y-%m-%d'))
        for mdate in date_list:
            params['sectionTime'] = mdate
            yield FormRequest(url = self.start_url, method = 'POST', meta={'cur_date': mdate}, formdata = params, callback = self.parse, errback=self.errback_httpbin)

    def get_change_date(self, row):
        if row['thr_change']: return row['thr_change']
        if row['sec_change']: return row['sec_change']
        if row['fir_change']: return row['fir_change']
        return ''

    def str2int(self, mdate):
        return mdate.replace("-", "").replace("00000000", "")

    def parse(self, response):
        try:
            if response.status == 200:
                cur_date = response.meta['cur_date']
                jsonstr = response.text
                info = json.loads(jsonstr)
                df = pd.DataFrame(info["prbookinfos"])
                df.columns = ["report_date", "first", "fir_change", "sec_change", "thr_change", "actual", "org_code", "code", "name"]
                df['change'] = df.apply(lambda row: self.get_change_date(row), axis = 1)
                df = df[['code', 'first', 'change', 'actual']]
                df['first'] = df['first'].apply(lambda row: self.str2int(row))
                df['actual'] = df['actual'].apply(lambda row: self.str2int(row))
                df['change'] = df['change'].apply(lambda row: self.str2int(row))
                df = df.sort_values(['code'], ascending = 1)
                filepath = os.path.join(ct.STOCK_FINANCIAL_REPORT_ANNOUNCEMENT_DATE_PATH, "%s.csv" % cur_date)
                df.to_csv(filepath, index=False, mode="w", encoding='utf8')
                self.status = True
                self.message = "scraped {} disclosure info succeed".format(len(df))
        except Exception as e:
            message = "get disclosure info exception:{}".format(e)
            self.status = False
            self.message = message
            self.logger.error(message)

    def spider_closed(self, spider, reason):
        self.collect_spider_info()
        self.send_message(self.name, "status:{}, message:{}".format(self.status, self.message))
