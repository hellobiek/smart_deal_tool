#!/usr/bin/python
# coding=utf-8
import urllib, urllib2, cookielib, ssl
import os,time,string
import sys
from log import getLogger
from const import LOGIN_URL

class Client:
    def __init__(self):
        self.log = getLogger(__name__)
        self.my_cookie = ""

    #########################get html and base cookie ######################
    def prepare(self, first_url = LOGIN_URL):
        ssl._create_default_https_context = ssl._create_unverified_context
        tmp_cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(tmp_cookie))
        try:
            response = opener.open(first_url, timeout=10)
        except urllib2.HTTPError, e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5, None
        except urllib2.URLError, e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception, e:
            self.log.warn("other exception: msg=%s", e.message)
            return -100, None
        for item in tmp_cookie:
            self.my_cookie+=item.name + "=" +item.value + ";"
        return 0, None

    ########## post data to request_url ##############
    def post(self, request_url, post_data):
        post_encode = urllib.urlencode(post_data)
        self.log.info("request_url:%s, post_data:%s" % (request_url,post_data))
        req = urllib2.Request(
            url=request_url,
            data=post_encode
            )
        req.add_header('Cookie', self.my_cookie)
        try:
            resp = urllib2.urlopen(req, timeout=10)
        except urllib2.HTTPError, e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5, None
        except urllib2.URLError, e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception, e:
            self.log.warn("other exception: msg=%s", e.message)
            return -100, None
        htm = resp.read()
        return 0, htm

    ########## get data to request_url ###############
    def get(self, request_url, get_data, headers=None):
        tmp_url = request_url
        if get_data:
            tmp_url = tmp_url + "?" + urllib.urlencode(get_data)
        self.log.info("tmp_url:%s, cookie:%s" % (tmp_url,self.my_cookie))
        headers_info={
            'Accept':'*/*',
            'Cookie':self.my_cookie,
            'User-agent':'Mozilla/5.0',
        }
        if headers:
            for key in headers:
                headers_info[key] = headers[key]
        req = urllib2.Request(
            url = tmp_url,
            headers = headers_info
        )
        self.log.info("headers:%s." % req.headers)
        try:
            resp = urllib2.urlopen(req, timeout=10)
        except urllib2.HTTPError, e:
            self.log.warn("server process request error: err_code=%s", e.code)
            return -5, None
        except urllib2.URLError, e:
            self.log.warn("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception, e:
            self.log.warn("other exception: msg=%s", e.message)
            return -100, None
        return 0,resp.read()
