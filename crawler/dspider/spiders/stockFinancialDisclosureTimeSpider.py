# -*- coding: utf-8 -*-
import os
import re
import time
import requests
import datetime
import const as ct
import pandas as pd
from datetime import datetime
from scrapy import FormRequest
from pyquery import PyQuery as pq
from scrapy.spiders.crawl import Rule
from dspider.myspider import BasicSpider
from base.cdate import report_date_list_with
from scrapy.linkextractors import LinkExtractor
from dspider.straight_flush import StraightFlushSession
from dspider.items import StockFinancialDisclosureTimeItem
class StockFinancialDisclosureTimeSpider(BasicSpider):
    name = 'stockFinancialDisclosureTimeSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 5.0,
        'CONCURRENT_REQUESTS_PER_IP': 3,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'SPLASH_URL': 'http://scrapy-splash-container:8050',
        'DOWNLOADER_MIDDLEWARES': {
            'dspider.random_user_agent.RandomUserAgent': 100,
            'dspider.random_proxy.RandomProxy':200,
            #'scrapy_splash.SplashCookiesMiddleware': 723,
            #'scrapy_splash.SplashMiddleware': 725,
            #'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
            #will not start this agent middleware
            'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None 
        },
        'DUPEFILTER_CLASS': 'scrapy_splash.SplashAwareDupeFilter',
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'SPIDERMON_EXPECTED_FINISH_REASONS': ct.SPIDERMON_EXPECTED_FINISH_REASONS,
        #'SPIDERMON_VALIDATION_MODELS': {
        #    ChinaSecurityIndustryValuationItem: 'dspider.validators.PlateValuationModel',
        #},
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        #'ITEM_PIPELINES': {
        #    'dspider.pipelines.PlateValuationDownloadPipeline': 100,
        #    'dspider.pipelines.ChinaSecurityIndustryValuationHandlePipeline': 200,
        #},
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        ),
        'DEFAULT_REQUEST_HEADERS': {
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        },
        'USER_AGENTS': ct.USER_AGENTS,
        'TOR_CONTROL_PORT': 9051,
        'TOR_HOSTNAME': 'dockerhost',
        'TOR_PASSWORD': '123456',
        #'TOR_PASSWORD': '16:F742EB8268E91D0D60816CF4FDD97D2C2F37B7E71986C1ED39705BA6C6',
        'SIGNEWNYM_RATE': 60, # new ip rate, minimal value is 10 (seconds)
        'NEW_IP_HTTP_CODES': [502, 503, 504, 522, 524, 408, 429, 403],
        'HTTP_PROXY': 'http://tor-privoxy-container:8118'#使用Tor
    }
    data_dict = dict()
    sfsession = StraightFlushSession()
    allowed_domains = ['data.10jqka.com.cn', 's.thsi.cn']
    start_urls = ['https://s.thsi.cn/js/chameleon/time.{}.js', 'http://data.10jqka.com.cn/financial/yypl/date/{}/board/ALL/field/stockcode/order/DESC/page/{}/ajax/1/']
    repatten = 'http://data.10jqka.com.cn/financial/yypl/date/(.+?)/board/ALL/field/stockcode/order/DESC/page/(.+?)/ajax/1/'

    def start_requests(self):
        for mdate in report_date_list_with():
            while not self.update_cookie(): time.sleep(1)
            self.data_dict[mdate] = list()
            mcookie = {"v": self.sfsession.encode()}
            page_url = self.start_urls[1].format(mdate, 1)
            print(page_url)
            time.sleep(5)
            yield FormRequest(url = page_url, cookies = mcookie, method = 'GET', callback = self.parse_item)

    def update_cookie(self):
        time_stamp = int(time.time())
        time_url = self.start_urls[0].format(time_stamp/1200)
        response = requests.get(time_url)
        if response.status_code == 200:
            server_time = float(response.text.split('=')[1].split(';')[0])
            self.sfsession.update_server_time(server_time)
            return True
        return False

    def get_max_page(self, doc):
        span_text = doc("div.m-page.J-ajax-page span").text()
        last_page = span_text.split("/")
        max_page = int(last_page[1])
        return max_page

    def update_data(self, doc, cur_date):
        tr_node = doc("table tbody tr")
        for tr in tr_node.items():
            code = tr.children("td").eq(1).text().strip(' ')    #股票代码
            first  = tr.children("td").eq(3).text().strip(' ')  # 首次预约时间
            changed = tr.children("td").eq(4).text().strip(' ') # 变更时间
            actual = tr.children("td").eq(5).text().strip(' ')  # 实际披露时间
            first = first.replace("-", "").replace("00000000", "")
            changed = changed.replace("-", "")
            actual = actual.replace("-", "").replace("00000000", "")
            self.data_dict[cur_date].append([code, first, changed, actual])

    def parse_item(self, response):
        try:
            url = response.url
            status = response.status
            if status != 200: print("failed url:%s" % url)
            reg = re.compile(self.repatten)
            if reg.search(url) is not None:
                doc = pq(response.text)
                max_page = self.get_max_page(doc)
                cur_date, cur_page = reg.search(url).groups()
                cur_page = int(cur_page)
                self.update_data(doc, cur_date)
                if cur_page <= max_page:
                    while not self.update_cookie(): time.sleep(1)
                    cur_page += 1
                    mcookie = {"v": self.sfsession.encode()}
                    page_url = self.start_urls[1].format(cur_date, cur_page)
                    print(page_url)
                    yield FormRequest(url = page_url, cookies = mcookie, method = 'GET', callback = self.parse_item)
                else:
                    self.store_items(cur_date)
            else:
                print("%s url is not good" % url)
        except:
            print(e)

    def store_items(self, cur_date):
        df = pd.DataFrame(self.data_dict[cur_date], columns=["code", "first", "change", "actual"])
        df = df.sort_values(['code'], ascending = 1)
        filepath = os.path.join(ct.STOCK_FINANCIAL_REPORT_ANNOUNCEMENT_DATE_PATH, "%s.csv" % cur_date)
        print(filepath)
        df.to_csv(filepath, index=False, mode="w", encoding='utf8')
        self.data_dict[cur_date].clear()
