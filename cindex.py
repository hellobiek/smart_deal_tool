#coding=utf-8
import _pickle
import const as ct
from combination import Combination
from log import getLogger
logger = getLogger(__name__)

class CIndex(Combination):
    def __init__(self, dbinfo, code):
        Combination.__init__(self, dbinfo, code)

    @staticmethod
    def get_dbname(code):
        return "i%s" % code

    def run(self, data):
        if not data.empty:
            self.redis.set(self.get_redis_name(self.get_dbname(self.code)), _pickle.dumps(data, 2))
            self.influx_client.set(data)

if __name__ == '__main__':
    av = CIndex(ct.DB_INFO, '000001')
