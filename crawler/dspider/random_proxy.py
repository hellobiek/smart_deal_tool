'''
这个类主要用于产生随机代理
'''
import time
import socket
from stem import Signal
from stem.control import Controller
class RandomProxy(object):
    _last_signewnym_time = time.time()
    def __init__(self, proxy, tor_hostname, tor_port, password, signewnym_rate, new_ip_http_codes):
        self.proxy = proxy
        self.tor_port = tor_port
        self.tor_ip = socket.gethostbyname(tor_hostname)
        self.password = password
        self.signewnym_rate = signewnym_rate
        self.new_ip_http_codes = set(int(x) for x in new_ip_http_codes)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            proxy = crawler.settings.get('HTTP_PROXY'),
            tor_port = crawler.settings.get('TOR_CONTROL_PORT'),
            tor_hostname = crawler.settings.get('TOR_HOSTNAME'),
            password = crawler.settings.get('TOR_PASSWORD'),
            signewnym_rate = crawler.settings.get('SIGNEWNYM_RATE'),
            new_ip_http_codes = crawler.settings.getlist('NEW_IP_HTTP_CODES')
        )

    def set_new_ip(self):
        with Controller.from_port(address = self.tor_ip, port = self.tor_port) as controller:
            try:
                controller.authenticate(password=self.password)
                print("stem set new ip success")
                flag = controller.is_newnym_available()
                if flag: controller.signal(Signal.NEWNYM)
            except Exception as e:
                print(e)

    def process_request(self, request, spider):
        if time.time() - self._last_signewnym_time > self.signewnym_rate:
            self._last_signewnym_time = time.time()
            self.set_new_ip()
        request.meta['proxy'] = self.proxy

    def process_response(self, request, response, spider):
        if (response.status in self.new_ip_http_codes and time.time() - self._last_signewnym_time > self.signewnym_rate):
            self._last_signewnym_time = time.time()
            self.set_new_ip()
        return response
