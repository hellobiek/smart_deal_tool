#!/usr/local/bin/python3 -u
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
from gevent.pool import Pool
import gevent.pywsgi
import sys,time
from log import getLogger
from stock import StockManager
from optparse import OptionParser

parser = OptionParser(usage="./%prog [host][port]", version="%prog v0.1")
parser.add_option("-H", "--host", default="0.0.0.0", help="default: %default", metavar="HOST")
parser.add_option("-P", "--port", default=20041, type="int", help="default: %default", metavar="PORT")
(options, args) = parser.parse_args()

log = getLogger(__name__)
s = StockManager()

def init(sleep=0):
    while True:
        s.init()
        time.sleep(sleep)

def get_stock_realtime_info(sleep=0):
    while True:
        s.set_realtime_stock_info()
        time.sleep(sleep)

def get_realtime_index_info(sleep=0):
    while True:
        s.set_realtime_index_info()
        time.sleep(sleep)

def get_realtime_static_info(sleep=0):
    while True:
        s.set_realtime_static_info()
        time.sleep(sleep)

def application(environ, start_response):
    headers = [("Content-type", "text/html")]
    (_, _, path_info, _, _, _) = urlparse(environ["PATH_INFO"])
    start_response("200 OK", headers)
    return status_handler(environ, start_response)

def main():
    gevent.spawn_later(5, init, 28800)
    #gevent.spawn_later(10, get_stock_realtime_info, 10)
    #gevent.spawn_later(15, get_realtime_index_info, 15)
    #gevent.spawn_later(20, get_realtime_static_info, 20)
    log.info("serving on port %s:%s." % (options.host, options.port))
    httpd = gevent.pywsgi.WSGIServer((options.host, options.port), application)
    httpd.serve_forever()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        sys.exit(0)
        
