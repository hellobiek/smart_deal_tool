# -*- coding: utf-8 -*-
import os, json
import datetime
import const as ct
import pandas as pd
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
    params = {
        "sectionTime": "",
        "firstTime": "",
        "lastTime": "",
        "market": "szsh" ,
        "stockCode": "",
        "orderClos": "",
        "isDesc": "",
        "pagesize": "10000",
        "pagenum": "1",
    }
    def start_requests(self):
        date_list = one_report_date_list(datetime.now().strftime('%Y-%m-%d'))
        for mdate in date_list:
            self.params['sectionTime'] = mdate
            yield FormRequest(url = self.start_url, method = 'POST', meta={'cur_date': mdate}, formdata = self.params, callback = self.parse, errback=self.errback_httpbin)

    def get_change_date(self, row):
        if row['thr_change']: return row['thr_change']
        if row['sec_change']: return row['sec_change']
        if row['fir_change']: return row['fir_change']
        return ''

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
                df = df.sort_values(['code'], ascending = 1)
                filepath = os.path.join(ct.STOCK_FINANCIAL_REPORT_ANNOUNCEMENT_DATE_PATH, "%s.csv" % cur_date)
                df.to_csv(filepath, index=False, mode="w", encoding='utf8')
                message = 'scraped {} stock at {}'.format(len(df), datetime.now().strftime('%Y-%m-%d'))
                self.logger.info("{} {}".format(self.name, message))
                self.message_client.send_message(self.name, message)
        except Exception as e:
            self.logger.error("execption:{}".format(e))
