# coding=utf-8
import cmysql
import const as ct
from cindex import CIndex
from log import getLogger
logger = getLogger(__name__)

class IndexInfo:
    def __init__(self, dbinfo):
        self.mysql_client = cmysql.CMySQL(dbinfo)
        self.mysql_dbs = self.mysql_client.get_all_databases()
        if not self.init(): raise Exception("init index info table failed")

    def init(self):
        failed_list = list()
        for code in ct.INDEX_DICT:
            dbname = CIndex.get_dbname(code)
            if dbname not in self.mysql_dbs:
                if not self.mysql_client.create_db(dbname):
                    failed_list.append(code)
        if len(failed_list) > 0 :
            logger.error("%s create failed" % failed_list)
            return False
        return True

if __name__ == '__main__':
    ii = IndexInfo(ct.DB_INFO)
