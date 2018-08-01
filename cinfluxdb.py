#encoding=utf-8
from influxdb import DataFrameClient
class CInflux:
    def __init__(self, dbinfo, dbname):
        self.dbname = dbname
        self.client = DataFrameClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], dbname)

    def get(self):
        return self.client.query("select * from %s" % self.dbname)

    def set(self, df):
        return self.client.write_points(df, self.dbname, protocol='json')

    def create(self):
        return self.client.create_dbname(self.dbname)

    def delete(self):
        return self.client.drop_dbname(self.dbname)
