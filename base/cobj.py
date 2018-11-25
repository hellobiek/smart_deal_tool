#coding=utf-8
import const as ct
from cmysql import CMySQL
from base.cclass import CClass
from common import create_redis_obj
class CMysqlObj(CClass):
    __slots__ = ("code", "redis", "mysql_client")
    def __init__(self, code, dbname, dbinfo, redis_host):
        self.code           = code
        self._dbname        = dbname
        self.redis          = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client   = CMySQL(dbinfo = dbinfo, dbname = dbname, iredis = self.redis)

    def __del__(self):
        self.redis        = None
        self.mysql_client = None

    def create_db(self, db_name):
        return self.mysql_client.create_db(db_name)

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return table_name in set(table.decode() for table in self.redis.smembers(self.dbname))
        return False

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(tdate.decode() for tdate in self.redis.smembers(table_name))
        return False
    
    def get_existed_keys_list(self, table_name):
        if self.redis.exists(table_name):
            return list(tdate.decode() for tdate in self.redis.smembers(table_name))
        return list()

    @property
    def dbname(self):
        return self._dbname

    @dbname.setter
    def dbname(self, value):
        self._dbname = value
