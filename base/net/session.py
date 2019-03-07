# coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import ssl
import copy
import json
import requests
from urllib import parse 
from log import getLogger
from requests.exceptions import ReadTimeout, Timeout
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
class SessionClient():
    NUM_RETRY_REQUESTS = 3
    def __init__(self, headers):
        self.headers = headers
        self.cookies = None
        self.connect_timeout = 10
        self.log = getLogger(__name__)
        self.session = requests.Session()

    def _send_request(self, req_type, url, timeout, data = None, files = None):
        cookie_value = ''
        for item in self.cookies: cookie_value += item.name + "=" + item.value + ";"
        headers = copy.deepcopy(self.headers)
        if 'Cookie' in headers:
            headers['Cookie'] += cookie_value
        else:
            headers['Cookie'] = cookie_value
        for req_try in range(self.NUM_RETRY_REQUESTS):
            try:
                if req_type == 'GET':
                    response = self.session.get(url, headers=headers, timeout=timeout, verify = False)
                else:
                    post_encode = parse.urlencode(data).encode("utf-8")
                    if files is None:
                        response = self.session.post(url, data = post_encode, headers = headers, timeout = timeout, verify = False)
                    else:
                        response = self.session.post(url, headers = headers, timeout = timeout, files = files, verify = False)
            except (ReadTimeout, Timeout) as error:
                self.log.debug(error)
                continue
            except e as error:
                self.log.error(error)
                return -1, error
            if response.status_code == 200: 
                return 0, response
        return -1, 'time out'

    def _format__header_params(self, extra_value = None):
        headers = copy.deepcopy(self.headers)
        cookie_value = "" if extra_value is None else extra_value
        for item in self.cookies: cookie_value += item.name + "=" + item.value + ";"
        if 'Cookie' in headers:
            headers['Cookie'] += cookie_value
        else:
            headers['Cookie'] = cookie_value
        return headers

    def logout(self, url, extra_value = None):
        headers = self._format__header_params(extra_value)
        response = self.session.get(url, headers = headers, timeout = self.connect_timeout, verify = False)
        if response.status_code != 200: return response.status_code, response.reason
        self.cookies = None
        return 0, ''

    def login(self, url, post_data, extra_value = None):
        headers = self._format__header_params(extra_value)
        post_encode = parse.urlencode(post_data).encode("utf-8")
        response = self.session.post(url, data=post_encode, headers = headers, timeout = self.connect_timeout, verify = False)
        if response.status_code != 200: return response.status_code, response.reason
        self.cookies = response.cookies
        return 0, ''

    def post(self, url, data, timeout = 10, files = None):
        return self._send_request('POST', url, data = data, timeout = timeout, files=files)

    def get(self, url, timeout = 10):
        return self._send_request('GET', url, timeout = timeout)
