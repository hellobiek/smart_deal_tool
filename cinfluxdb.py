#encoding=utf-8
from influxdb import DataFrameClient
from common import create_redis_obj
ALL_IN_DATABASES = 'all_in_databases'
class CInflux:
    def __init__(self, dbinfo, dbname, iredis = None):
        self.dbname = dbname
        self.redis = create_redis_obj() if iredis is None else iredis
        self.df_client = DataFrameClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], self.dbname)

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
        dbname = dbname if dbname is not None else self.dbname
        return self.df_client.query("select * from %s" % dbname)

    def get_newset_row(self, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        return self.df_client.query("select last(*) from %s" % dbname)

    def set(self, df, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        return self.df_client.write_points(df, dbname, protocol='json')
    
    def create(self, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        if dbname in self.get_all_databases(): return True
        self.df_client.create_database(dbname)
        self.redis.sadd(ALL_IN_DATABASES, dbname)
        return True

    def delete(self, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        if dbname not in self.get_all_databases(): return 
        self.df_client.drop_database(dbname)
        self.redis.srem(ALL_IN_DATABASES, dbname)
