#!/usr/bin/python
# coding=utf-8
import os,time,string
import ssl
from urllib import parse 
from urllib import request
from http import cookiejar
from six.moves import html_parser
from log import getLogger
from const import LOGIN_URL

class Client:
    def __init__(self):
        self.log = getLogger(__name__)
        self.cookie = cookiejar.CookieJar()
        self.handler = request.HTTPCookieProcessor(self.cookie)
        self.opener = request.build_opener(self.handler)

    def prepare(self, first_url = LOGIN_URL):
        try:
            response = self.opener.open(first_url, timeout=10)
        except request.HTTPError as e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5
        except request.URLError as e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10
        except Exception as e:
            self.log.warn("other exception: msg=%s", e.reason)
            return -100
        return 0

    def post(self, request_url, post_data):
        post_encode = parse.urlencode(post_data).encode("utf-8")
        self.log.debug("url:%s, data:%s" % (request_url, post_data))
        req = request.Request(url=request_url, data=post_encode)
        try:
            resp = self.opener.open(req, timeout=10).read()
        except request.HTTPError as e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5, None
        except request.URLError as e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception as e:
            self.log.warn("other exception: msg=%s", e)
            return -100, None
        return 0, resp

    def get(self, request_url, get_data=None, headers=None):
        tmp_url = request_url
        if get_data: tmp_url = tmp_url + "?" + parse.urlencode(get_data)
        self.log.debug("url:%s, cookie:%s" % (tmp_url, self.cookie))
        headers_info={
            'Accept':r'*/*',
            'User-agent':r'Mozilla/5.0'
        }
        if headers:
            for key in headers:
                headers_info[key] = headers[key]
        req = request.Request(url = tmp_url, headers = headers_info)
        self.log.debug("headers:%s." % req.headers)
        try:
            resp = self.opener.open(req, timeout=10).read()
        except request.HTTPError as e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5, None
        except request.URLError as e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception as e:
            self.log.warn("other exception: msg=%s", e)
            return -100, None
        return 0, resp
