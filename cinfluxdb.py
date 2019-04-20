#encoding=utf-8
from base.clog import getLogger
from common import create_redis_obj
from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBServerError
ALL_IN_DATABASES = 'all_in_databases'
logger = getLogger(__name__)
class CInflux:
    def __init__(self, dbinfo, dbname, iredis = create_redis_obj()):
        self.redis  = iredis
        self.dbname = dbname
        self.df_client = DataFrameClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], self.dbname, timeout=10)

    def __del__(self):
        self.redis = None
        self.df_client = None

    def get_all_databases(self):
        if self.redis.exists(ALL_IN_DATABASES):
            return set(str(dbname, encoding = "utf8") for dbname in self.redis.smembers(ALL_IN_DATABASES))
        else:
            all_dbs = self._get_all_databses()
            for _db in all_dbs: self.redis.sadd(ALL_IN_DATABASES, _db)
            return all_dbs

    def _get_all_databses(self):
        return self.df_client.get_list_database()

    def get(self, dbname = None):
        if dbname is None: dbname = self.dbname
        return self.df_client.query("select * from %s" % dbname)

    def get_newset_row(self, dbname = None):
        if dbname is None: dbname = self.dbname
        return self.df_client.query("select last(*) from %s" % dbname)

    def set(self, df, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        try:
            self.df_client.write_points(df, dbname, protocol='json')
            return True
        except InfluxDBServerError as e:
            logger.error(e)
            return False
    
    def create(self, dbname = None):
        if dbname is None: dbname = self.dbname
        if dbname in self.get_all_databases(): return True
        self.df_client.create_database(dbname)
        self.redis.sadd(ALL_IN_DATABASES, dbname)
        return True

    def delete(self, dbname = None):
        if dbname is None: dbname = self.dbname
        if dbname not in self.get_all_databases(): return True
        self.df_client.drop_database(dbname)
        self.redis.srem(ALL_IN_DATABASES, dbname)
        return True
