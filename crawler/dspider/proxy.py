# -*- coding: utf-8 -*-
'''
这个类主要用于产生随机代理
'''
import json
import time
import random
import requests
from scrapy.http import TextResponse
from twisted.web.http import _DataLoss
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import TimeoutError, ConnectionRefusedError, ConnectError, ConnectionDone
class RandomProxy(object):
    _last_signewnym_time = time.time()
    def __init__(self, proxy_host, signewnym_rate, new_ip_http_codes):
        self.proxy_host = proxy_host
        self.signewnym_rate = signewnym_rate
        self.proxies = list()
        self.new_ip_http_codes = set(int(x) for x in new_ip_http_codes)
        self.update_proxy_list()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            proxy_host = crawler.settings.get('PROXY_HOST'),
            signewnym_rate = crawler.settings.get('SIGNEWNYM_RATE'),
            new_ip_http_codes = crawler.settings.getlist('NEW_IP_HTTP_CODES')
        )

    def update_proxy_list(self):
        try:
            response = requests.get("{}/get_all/".format(self.proxy_host))
            self.proxies = json.loads(response.text) if response.status_code == 200 else list()
        except Exception as e:
            self.proxies = list()
            print(e)

    def get_proxy(self):
        return random.choice(self.proxies) if len(self.proxies) > 0 else None

    def process_request(self, request, spider):
        if ((time.time() - self._last_signewnym_time > self.signewnym_rate) or (len(self.proxies) == 0)):
            self._last_signewnym_time = time.time()
            self.update_proxy_list()
        proxy = self.get_proxy()
        if proxy is not None:
            request.meta["proxy"] = "http://{}".format(proxy)
        else:
            print("using no proxy")

    def process_response(self, request, response, spider):
        if response.status != 200:
            print("unexpcted status:", response.url, response.status)
            proxy = request.meta['proxy'].split('//')[1]
            if proxy in self.proxies: self.proxies.remove(proxy)
            return TextResponse(url=request.url)
        return response

    def process_exception(self, request, exception, spider):
        DONT_RETRY_ERRORS = (TimeoutError, ConnectionRefusedError, ResponseNeverReceived, ConnectError, ValueError, TypeError, ConnectionDone, _DataLoss)
        if not isinstance(exception, DONT_RETRY_ERRORS):
            print("unknow exception: %s" % exception, type(exception))
        proxy = request.meta['proxy'].split('//')[1]
        if proxy in self.proxies: self.proxies.remove(proxy)
        return TextResponse(url=request.url)
