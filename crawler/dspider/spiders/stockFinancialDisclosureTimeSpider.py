# -*- coding: utf-8 -*-
import os
import re
import time
import datetime
import const as ct
import pandas as pd
from datetime import datetime
from scrapy import FormRequest
from scrapy.http import TextResponse, HtmlResponse
from pyquery import PyQuery as pq
from dspider.myspider import BasicSpider
from urllib.request import urlopen, Request
from base.cdate import report_date_list_with, one_report_date_list
from dspider.straight_flush import StraightFlushSession
from dspider.items import StockFinancialDisclosureTimeItem
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
class StockFinancialDisclosureTimeSpider(BasicSpider):
    name = 'stockFinancialDisclosureTimeSpider'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'RETRY_ENABLED': False,
        'REFERER_ENABLED': False,
        'SPIDERMON_ENABLED': False,
        'DOWNLOAD_DELAY': 5,
        'DOWNLOAD_TIMEOUT': 20.0,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOADER_MIDDLEWARES': {
            'dspider.proxy.RandomProxy':100,
            'dspider.user_agent.RandomUserAgent': 200,
            'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None
        },
        'USER_AGENTS': ct.USER_AGENTS,
        'SIGNEWNYM_RATE': 60, # new ip rate, minimal value is 60 (seconds)
        'PROXY_HOST': 'http://ip_proxy-container:5010',
        'NEW_IP_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 403, 407, 404]
    }
    data_dict = dict()
    #date_list = report_date_list_with()
    date_list = one_report_date_list(datetime.now().strftime('%Y-%m-%d'))
    sfsession = StraightFlushSession()
    allowed_domains = ['data.10jqka.com.cn', 's.thsi.cn']
    start_urls = ['https://s.thsi.cn/js/chameleon/time.{}.js', 'http://data.10jqka.com.cn/financial/yypl/date/{}/board/ALL/field/stockcode/order/DESC/page/{}/ajax/1/']
    repatten = 'http://data.10jqka.com.cn/financial/yypl/date/(.+?)/board/ALL/field/stockcode/order/DESC/page/(.+?)/ajax/1/'
    headers = {"Accept-Language": "en-US,en;q=0.5","Connection": "keep-alive"}
    def start_requests(self):
        if len(self.date_list) > 0:
            while not self.update_cookie(): time.sleep(3)
            mdate = self.date_list.pop()
            self.data_dict[mdate] = list()
            mcookie = {"v": self.sfsession.encode()}
            page_url = self.start_urls[1].format(mdate, 1)
            self.logger.info("start_request:%s", page_url)
            yield FormRequest(url = page_url, headers = self.headers, cookies = mcookie, method = 'GET', callback = self.parse_item)

    def parse_item(self, response):
        try:
            url = response.url
            self.update_cookie()
            mcookie = {"v": self.sfsession.encode()}
            if type(response) is TextResponse:
                time.sleep(60)
                print("parse_item3", response.url)
                yield FormRequest(url = url, headers = self.headers, cookies = mcookie, method = 'GET', callback = self.parse_item, errback=self.errback_httpbin, dont_filter=True)
            else:
                reg = re.compile(self.repatten)
                if reg.search(url) is not None:
                    doc = pq(response.text)
                    max_page = self.get_max_page(doc)
                    cur_date, cur_page = reg.search(url).groups()
                    cur_page = int(cur_page)
                    if not self.update_data(doc, cur_date): print("empty url", url)
                    if cur_page < max_page:
                        cur_page += 1
                        page_url = self.start_urls[1].format(cur_date, cur_page)
                        print("parse_item1", page_url)
                        yield FormRequest(url = page_url, headers = self.headers, cookies = mcookie, method = 'GET', callback = self.parse_item, errback=self.errback_httpbin)
                    else:
                        self.store_items(cur_date)
                        if len(self.date_list) > 0:
                            mdate = self.date_list.pop()
                            self.data_dict[mdate] = list()
                            page_url = self.start_urls[1].format(mdate, 1)
                            print("parse_item2", page_url)
                            yield FormRequest(url = page_url, headers = self.headers, cookies = mcookie, method = 'GET', callback = self.parse_item, errback=self.errback_httpbin)
                else:
                    print("parse_item4", url)
                    yield FormRequest(url = url, headers = self.headers, cookies = mcookie, method = 'GET', callback = self.parse_item, errback = self.errback_httpbin, dont_filter = True)
        except:
            print("parse_item exception", e)

    def errback_httpbin(self, failure):
        print("errback", repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            print('HttpError on %s', response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            print('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError):
            request = failure.request
            print('TimeoutError on %s', request.url)
        else:
            request = failure.request
            print('Other Error on %s', request.url)

    def store_items(self, cur_date):
        df = pd.DataFrame(self.data_dict[cur_date], columns=["code", "first", "change", "actual"])
        df = df.sort_values(['code'], ascending = 1)
        filepath = os.path.join(ct.STOCK_FINANCIAL_REPORT_ANNOUNCEMENT_DATE_PATH, "%s.csv" % cur_date)
        df.to_csv(filepath, index=False, mode="w", encoding='utf8')
        self.data_dict[cur_date].clear()

    def update_cookie(self):
        self.sfsession = StraightFlushSession()
        time_stamp = int(time.time())
        time_url = self.start_urls[0].format(int(time_stamp/1200))
        request = Request(time_url)
        request.add_header("Connection", "close")
        request.add_header("Accept-Language", "en-US,en;q=0.5")
        request.add_header("User-Agent", 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36')
        try:
            response = urlopen(request, timeout=50)
            if response.status == 200:
                server_time = float(response.read().decode("utf-8").split('=')[1].split(';')[0])
                self.sfsession.update_server_time(server_time)
                return True
        except Exception as e:
            print("update_cookie", e)
        return False

    def get_max_page(self, doc):
        span_text = doc("div.m-page.J-ajax-page span").text()
        last_page = span_text.split("/")
        max_page = int(last_page[1])
        return max_page

    def update_data(self, doc, cur_date):
        tr_node = doc("table tbody tr")
        if tr_node.length == 0: return False
        for tr in tr_node.items():
            code = tr.children("td").eq(1).text().strip(' ')    #股票代码
            first  = tr.children("td").eq(3).text().strip(' ')  # 首次预约时间
            changed = tr.children("td").eq(4).text().strip(' ') # 变更时间
            actual = tr.children("td").eq(5).text().strip(' ')  # 实际披露时间
            first = first.replace("-", "").replace("00000000", "")
            changed = changed.replace("-", "")
            actual = actual.replace("-", "").replace("00000000", "")
            self.data_dict[cur_date].append([code, first, changed, actual])
        return True
