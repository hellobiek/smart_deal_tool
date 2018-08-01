#encoding=utf-8
import traceback
import const as ct
import pandas as pd
from log import getLogger
from pandas import DataFrame
from common import create_redis_obj
from influxdb import SeriesHelper
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

log = getLogger(__name__)
ALL_TABLES = 'all_tables'
ALL_TRIGGERS = 'all_triggers'
class CInflux:
    def __init__(self, dbinfo):
        self.redis = create_redis_obj()
        self.client = InfluxDBClient(dbinfo, protocol)
        self.p_client = InfluxDBClient(dbinfo, protocol)

    def __del__(self):
        self.client.close()
        self.p_client.close()
        self.redis.connection_pool.disconnect()

    def get_all_tables(self):
        if self.redis.exists(ALL_TABLES):
            return set(str(table, encoding = "utf8") for table in self.redis.smembers(ALL_TABLES))
        else:
            all_tables = self.client.get_list_measurements()
            for table in all_tables: self.redis.sadd(ALL_TABLES, table)
            return all_tables

    def set(self, data_frame, table, dbname, method = ct.APPEND)
        
def main(dbinfo, protocol):
    """Instantiate the connection to the InfluxDB client."""
    client = DataFrameClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], dbname)
    print("Create pandas DataFrame")
    df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])
    print("Create database: " + dbname)
    client.create_database(dbname)
    print("Write DataFrame")
    client.write_points(df, dbname, protocol=protocol)
    print("Write DataFrame with Tags")
    client.write_points(df, dbname, {'k1': 'v1', 'k2': 'v2'}, protocol=protocol)
    print("Read DataFrame")
    client.query("select * from %s" % dbname)
    #print("Delete database: " + dbname)
    #client.drop_database(dbname)

if __name__ == '__main__':
    try:
        main(ct.IN_DB_INFO, 'json', 'stock')
    except Exception as e:
        traceback.print_exc()
