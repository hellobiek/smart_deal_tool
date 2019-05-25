# -*- coding: utf-8 -*-
import re
import datetime
import const as ct
from datetime import datetime
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import ChinaTreasuryRateItem
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
    name = 'chinaTreasuryRateSpider'
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
    allowed_domains = ['yield.chinabond.com.cn']
    start_url = "http://yield.chinabond.com.cn/cbweb-pbc-web/pbc/queryGjqxInfo"
    def start_requests(self):
        mformat = '%Y%m%d'
        formdata = dict()
        formdata['locale'] = 'cn_ZH'
        end_date = datetime.now().strftime(mformat)
        #start_date = self.get_nday_ago(end_date, 4835, dformat = mformat)
        start_date = self.get_nday_ago(end_date, 10, dformat = mformat)
        while start_date < end_date:
            start_date = self.get_tomorrow_date(sdate = start_date, dformat = mformat)
            formdata['workTime'] = start_date
            yield FormRequest(url = self.start_url, method = 'GET', formdata = formdata, callback = self.parse)

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
