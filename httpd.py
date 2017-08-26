#!/home/tops/bin/python -u
import sys
from log import getLogger
from data_collect import StockManager
try:
    import gevent
except ImportError:
    print "error: httpd require gevent package."
    sys.exit(1)
from gevent import monkey
from gevent.pool import Pool
import gevent.wsgi
monkey.patch_all(subprocess=True)

parser = OptionParser(usage="./%prog [host][port]", version="%prog v0.1")
parser.add_option("-H", "--host", default="0.0.0.0", help="default: %default", metavar="HOST")
parser.add_option("-P", "--port", default=8000, type="int", help="default: %default", metavar="PORT")
(options, args) = parser.parse_args()

def init(sleep=0):
    #init data for trade
    logger.info("start init data for trade.")
    while True:
        try:
            s = StockManager()
        except:
            logger.error("init data exception.", exc_info=True)
        time.sleep(sleep)

def main():
    gevent.spawn_later(25,init_data,300)
    httpd = gevent.wsgi.WSGIServer((options.host, options.port), application)
    print "Serving on port %s:%s." % (options.host, options.port)
    httpd.serve_forever()

if __name__ == "__main__":
    main()
