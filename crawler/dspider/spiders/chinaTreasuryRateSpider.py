# -*- coding: utf-8 -*-
import re
import datetime
import const as ct
import pandas as pd
from scrapy import signals
from datetime import datetime
from scrapy import FormRequest
from base.wechat import SendWechat
from dspider.myspider import BasicSpider
from dspider.items import ChinaTreasuryRateItem
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
treasury_rate_to_path = {
    "date"  :"/html[1]/body[1]/table[1]/tr[1]/th[1]/text()",#日期
    "name"  :"/html[1]/body[1]/table[1]/tr[2]/td[1]/text()",#名称
    "month3":"/html[1]/body[1]/table[1]/tr[2]/td[2]/text()",#3月
    "month6":"/html[1]/body[1]/table[1]/tr[2]/td[3]/text()",#6月
    "year1" :"/html[1]/body[1]/table[1]/tr[2]/td[4]/text()",#1年
    "year3" :"/html[1]/body[1]/table[1]/tr[2]/td[5]/text()",#3年
    "year5" :"/html[1]/body[1]/table[1]/tr[2]/td[6]/text()",#5年
    "year7" :"/html[1]/body[1]/table[1]/tr[2]/td[7]/text()",#7年
    "year10":"/html[1]/body[1]/table[1]/tr[2]/td[8]/text()",#10年
    "year30":"/html[1]/body[1]/table[1]/tr[2]/td[9]/text()" #30年
}

class ChinaTreasuryRateSpider(BasicSpider):
    name = 'treasuryRateSpider'
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
            'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 100,
            'dspider.pipelines.DspiderPipeline': 200
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_VALIDATION_MODELS': {
            ChinaTreasuryRateItem: 'dspider.validators.ChinaTreasuryRateModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    scraped_dates = [] 
    allowed_domains = ['yield.chinabond.com.cn']
    start_url = "http://yield.chinabond.com.cn/cbweb-pbc-web/pbc/queryGjqxInfo"
    def start_requests(self):
        mformat = '%Y%m%d'
        formdata = dict()
        formdata['locale'] = 'cn_ZH'
        self.scraped_dates = [] 
        end_date = datetime.now().strftime(mformat)
        start_date = self.get_nday_ago(end_date, 10, dformat = mformat)
        while start_date <= end_date:
            formdata['workTime'] = start_date
            yield FormRequest(url = self.start_url, method = 'GET', formdata = formdata, callback = self.parse, errback=self.errback_httpbin)
            start_date = self.get_tomorrow_date(sdate = start_date, dformat = mformat)

    def parse(self, response):
        item = ChinaTreasuryRateItem()
        for k in treasury_rate_to_path:
            value_str = response.xpath(treasury_rate_to_path[k]).extract_first()
            if k == 'date':
                value_str = "{}-{}-{}".format(value_str[0:4], value_str[4:6], value_str[6:8])
            elif k == 'name':
                value_str = '国债收益率'
            else:
                if value_str is not None: value_str = float(value_str)
            item[k] = value_str
        if not item.empty(): 
            yield item
            self.scraped_dates.append(item['date'])

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ChinaTreasuryRateSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def send_message(self):
        md = MarkdownWriter()
        mdate = datetime.now().strftime('%Y-%m-%d')
        title = "{} 爬虫信息".format(mdate)
        status_info = pd.read_csv(ct.SPIDER_STATUS_FILE)
        status_info['name'] = status_info['name'].str[0:-6]
        md.addHeader(title, 1)
        t_index = MarkdownTable(headers = ["名称", "状态", "时间"])
        for index in range(len(status_info)):
            data_list =  status_info.loc[index].tolist()
            content_list = [data_list[0], data_list[1], data_list[2]]
            content_list = ["{}\u3000".format(str(i)) for i in content_list]
            t_index.addRow(content_list)
        md.addTable(t_index)
        message = md.getStream()
        message_client = SendWechat()
        message_client.send_message(title, message)

    def spider_closed(self, spider, reason):
        mdate = datetime.now().strftime('%Y-%m-%d')
        if mdate in self.scraped_dates:
            message = "get treasury rate {} info succeed".format(mdate)
            self.status = True
        else:
            message = "get treasury rate {} info falied".format(mdate)
            self.status = False
        self.message = message
        self.collect_spider_info()
        self.send_message()
