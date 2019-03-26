# coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import ssl
from urllib import parse 
from base.clog import getLogger
from urllib import request
from http import cookiejar
class Client:
    def __init__(self):
        self.cookie_value = ""
        self.log = getLogger(__name__)
        self.context = ssl._create_unverified_context()
        self.cookie = cookiejar.MozillaCookieJar()
        self.ssl_handler = request.HTTPSHandler(context = self.context)
        self.cookie_handler = request.HTTPCookieProcessor(self.cookie)
        self.handlers = [self.ssl_handler, self.cookie_handler]
        self.opener = request.build_opener(*self.handlers)

    def prepare(self, url):
        try:
            response = self.opener.open(url, timeout=10)
            for item in self.cookie:
                self.cookie_value += item.name + "=" + item.value + ";"
        except request.HTTPError as e:
            self.log.warning("server process request error: err_code=%s", e.code)
            return -5
        except request.URLError as e:
            self.log.warning("reach server error: %s", e)
            return -10
        except Exception as e:
            self.log.warning("other exception: msg=%s", e)
            return -100
        return 0

    def post(self, request_url, post_data):
        post_encode = parse.urlencode(post_data).encode("utf-8")
        self.log.debug("url:%s, data:%s" % (request_url, post_data))
        req = request.Request(url=request_url, data=post_encode)
        req.add_header('Cookie', self.cookie_value)
        try:
            resp = self.opener.open(req, timeout=10).read()
        except request.HTTPError as e:
            self.log.warning("server process request error: err_code=%s", e.code)
            return -5, None
        except request.URLError as e:
            self.log.warning("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception as e:
            self.log.warning("other exception: msg=%s", e)
            return -100, None
        return 0, resp

    def get(self, request_url, get_data=None, headers=None):
        tmp_url = request_url
        if get_data: tmp_url = tmp_url + "?" + parse.urlencode(get_data)
        self.log.debug("url:%s, cookie:%s" % (tmp_url, self.cookie))
        headers_info = {
            'Accept':r'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-agent':r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
        }
        if headers:
            for key in headers:
                headers_info[key] = headers[key]
        req = request.Request(url = tmp_url, headers = headers_info)
        req.add_header('Cookie', self.cookie_value)
        self.log.debug("headers:%s." % req.headers)
        try:
            resp = self.opener.open(req, timeout=10).read()
        except request.HTTPError as e:
            self.log.warning("server process request error: err_code=%s", e.code)
            return -5, None
        except request.URLError as e:
            self.log.warning("reach server error: reason=%s", e.reason)
            return -10, None
        except Exception as e:
            self.log.warning("other exception: msg=%s", e)
            return -100, None
        return 0, resp
