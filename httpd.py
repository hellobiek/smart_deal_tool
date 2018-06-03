# coding=utf-8
#!/usr/local/bin/python3 -u
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
import gevent.pywsgi
import sys
import time
import const as ct
from log import getLogger
from chalted import CHalted
from data_manager import DataManager
from optparse import OptionParser

parser = OptionParser(usage="./%prog [host][port]", version="%prog v0.1")
parser.add_option("-H", "--host", default="0.0.0.0", help="default: %default", metavar="HOST")
parser.add_option("-P", "--port", default=20041, type="int", help="default: %default", metavar="PORT")
(options, args) = parser.parse_args()

log = getLogger(__name__)

def application(environ, start_response):
    headers = [("Content-type", "text/html")]
    (_, _, path_info, _, _, _) = urlparse(environ["PATH_INFO"])
    start_response("200 OK", headers)
    return status_handler(environ, start_response)

def main():
    dm = DataManager(ct.DB_INFO, ct.STOCK_INFO_TABLE, ct.COMBINATION_INFO_TABLE, ct.CALENDAR_TABLE, ct.DELISTED_INFO_TABLE)
    haltm = CHalted(ct.DB_INFO, ct.HALTED_TABLE, ct.STOCK_INFO_TABLE, ct.CALENDAR_TABLE) 
    gevent.spawn_later(3, haltm.run, 3400)  #collect suspeded stock info
    gevent.spawn_later(5, dm.prepare, 21600)
    gevent.spawn_later(120, dm.run, ct.C_STOCK, 0)
    #gevent.spawn_later(60, dm.run, ct.C_INDEX, 14400)
    log.info("serving on port %s:%s." % (options.host, options.port))
    httpd = gevent.pywsgi.WSGIServer((options.host, options.port), application)
    httpd.serve_forever()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        sys.exit(0)
