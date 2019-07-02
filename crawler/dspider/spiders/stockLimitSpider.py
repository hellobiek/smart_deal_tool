#coding=utf-8
import re
import time
import json
import const as ct
import numpy as np
import pandas as pd
from cmysql import CMySQL
from scrapy import Request
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from common import create_redis_obj
from dspider.myspider import BasicSpider
from dspider.items import StockLimitItem
from base.cdate import get_day_nday_ago, get_dates_array, transfer_int_to_date_string
class StockLimitSpider(BasicSpider):
    name = 'stocklimitspider'
    LIMIT_UP = 0
    LIMIT_DOWN = 1
    FIR_PAGE_ORDER = 'first'
    SEC_PAGE_ORDER = 'second'
    LIMIT_URL_PRIFIX = "http://home.flashdata2.jrj.com.cn/limitStatistic/"
    LIMIT_URL_MID = ".js?_dc="
    allowed_domains = ['home.flashdata2.jrj.com.cn']
    repatten = 'http://home.flashdata2.jrj.com.cn/limitStatistic/(.+?)/(.+?).js?'
    header = {"Host": "home.flashdata2.jrj.com.cn",
              "Referer": "http://stock.jrj.com.cn/tzzs/zdtwdj/zdforce.shtml",
              "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
    mdict = {
        "代码": "code", "价格": "price", "涨跌幅": "pchange",
        "振幅": "prange", "强度": "intensity", "封成比": "fcb",
        "封流比": "flb", "封单金额": "fdmoney", 
        "所属概念": "concept", "开板次数": "open_times",
        "首次涨跌停时间": "first_time",
        "最后涨跌停时间": "last_time"
    }
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
            StockLimitItem: 'dspider.validators.StockLimitModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }

    def convert_to_json(self, content):
        if 0 == len(content): return None
        p = re.compile(r'"Data":(.*)};', re.S)
        result = p.findall(content)
        return json.loads(result[0]) if result else None

    def get_url(self, dtype, mdate):
        mdate = datetime.strptime(mdate, "%Y-%m-%d").strftime("%Y%m%d")
        if self.LIMIT_UP == dtype:
            return self.LIMIT_URL_PRIFIX + "zt/%s" % mdate + self.LIMIT_URL_MID + str(int(round(time.time() * 1000)))
        else:
            return self.LIMIT_URL_PRIFIX + "dt/%s" % mdate + self.LIMIT_URL_MID + str(int(round(time.time() * 1000)))

    def get_useful_columns(self, dtype):
        if self.FIR_PAGE_ORDER == dtype:
            return ['代码', '所属概念']
        else:
            return ['代码', '价格', '涨跌幅', '振幅', '封成比', '封流比', '封单金额', '首次涨跌停时间', '最后涨跌停时间', '开板次数', '强度']

    def get_columns(self, dtype):
        #封成比 = 封单金额/日成交额
        #封流比 = 封单手数/流通股本
        if self.FIR_PAGE_ORDER == dtype:
            return ['代码', '名称', '涨跌停时间', '价格', '涨跌幅', '成交额', '振幅', '换手率', '五日涨跌幅', '无用', '所属概念（代码）', '所属概念']
        else:
            return ['代码', '名称', '价格', '涨跌幅', '封成比', '封流比', '封单金额', '首次涨跌停时间', '最后涨跌停时间', '开板次数', '振幅', '强度']

    def get_sub_url(self, dtype, mdate):
        if 'zt' == dtype:
            return self.LIMIT_URL_PRIFIX + "ztForce/%s" % mdate + self.LIMIT_URL_MID + str(int(round(time.time() * 1000)))
        else:
            return self.LIMIT_URL_PRIFIX + "dtForce/%s" % mdate + self.LIMIT_URL_MID + str(int(round(time.time() * 1000)))

    def start_requests(self):
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = self.get_nday_ago(end_date, 10, dformat = '%Y-%m-%d')
        date_array = get_dates_array(start_date, end_date)
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                up_url = self.get_url(self.LIMIT_UP, mdate) 
                yield Request(url=up_url, callback=self.parse, errback=self.errback_httpbin)
                down_url = self.get_url(self.LIMIT_DOWN, mdate) 
                yield Request(url=down_url, callback=self.parse, errback=self.errback_httpbin)

    def parse_sub_url(self, response):
        try:
            mdate = response.meta['date']
            mdate = transfer_int_to_date_string(mdate)
            limit_json_obj = response.meta['item']
            limit_df = pd.DataFrame(limit_json_obj, columns = self.get_columns(self.FIR_PAGE_ORDER))
            limit_df = limit_df[self.get_useful_columns(self.FIR_PAGE_ORDER)]
            if response.status == 200:
                content = response.text
                limit_intensity_json_obj = self.convert_to_json(content)
                limit_intensity_df = pd.DataFrame(limit_intensity_json_obj, columns=self.get_columns(self.SEC_PAGE_ORDER))
                limit_intensity_df = limit_intensity_df[self.get_useful_columns(self.SEC_PAGE_ORDER)]
                df = pd.merge(limit_intensity_df, limit_df, how='left', on=['代码'])
                df.replace(np.inf, 1000, inplace = True)
                df = df.reset_index(drop = True)
                df.columns = [self.mdict[key] for key in df.columns.tolist()] 
                df['date'] = mdate
                records = df.to_dict('records')
                for record in df.to_dict('records'): yield StockLimitItem(record)
            else:
                print("%s is None url" % response.url)
        except Exception as e:
            print(e)

    def parse(self, response):
        try:
            url = response.url
            if response.status == 200:
                content = response.text
                data = self.convert_to_json(content)
                reg = re.compile(self.repatten)
                if reg.search(url) is not None:
                    cstr, mdate = reg.search(url).groups()
                    sub_url = self.get_sub_url(cstr, mdate)
                    yield Request(url = sub_url, callback=self.parse_sub_url, errback=self.errback_httpbin, meta={'item': data, 'date': mdate})
            else:
                print("%s is None url" % url)
        except Exception as e:
            print(e)
