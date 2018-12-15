# coding=utf-8
from cindex import CIndex
from common import concurrent_run
import const as ct
class IndexInfo:
    def create_obj(self, code):
        try:
            CIndex(code, should_create_influxdb = True, should_create_mysqldb = True)
            return (code, True)
        except Exception as e:
            return (code, False)

    def update(self):
        return concurrent_run(self.create_obj, list(ct.TDX_INDEX_DICT.keys()), num = 10)
