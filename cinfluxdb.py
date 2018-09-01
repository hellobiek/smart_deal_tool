#encoding=utf-8
import datetime
from datetime import datetime
from common import unix_time_millis
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
class CInflux:
    def __init__(self, dbinfo, dbname):
        self.dbname = dbname
        #self.l2_dbname = "%s_l2" % self.dbname
        #self.client = InfluxDBClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], self.l2_dbname)
        self.df_client = DataFrameClient(dbinfo['host'], dbinfo['port'], dbinfo['user'], dbinfo['password'], self.dbname)

    def __del__(self):
        self.df_client = None

    def list_all_databases(self):
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
        self.df_client.create_database(self.dbname)

    def delete(self, dbname = None):
        dbname = dbname if dbname is not None else self.dbname
        self.df_client.drop_database(dbname)

    #def l2_create(self):
    #    self.client.create_database(self.l2_dbname)

    #def l2_get_points(self, csvfile, metric, fieldcolumns, tagcolumns, delimiter):
    #    datapoints = []
    #    with open(filename, 'r') as csvfile:
    #        reader = csv.DictReader(csvfile, delimiter=delimiter)
    #        for row in reader:
    #            timestamp = unix_time_millis(datetime.strptime(row[timecolumn], timeformat)) * 1000000
    #            tags = {}
    #            for t in tagcolumns:
    #                v = 0
    #                if t in row: v = row[t]
    #                tags[t] = v
    #                
    #            fields = {}
    #            for f in fieldcolumns:
    #                v = 0
    #                if f in row: v = float(row[f]) if isfloat(row[f]) else row[f]
    #                fields[f] = v
    #            point = {"measurement": metric, "time": timestamp, "fields": fields, "tags": tags}
    #            datapoints.append(point) 
    #    return datapoints

    #def write_l2_csv(self, csvfile, metric, fieldcolumns = ['time', 'price', 'direction', 'volume'], tagcolumns = list(), delimiter=','):
    #    datapoints = self.get_points(csvfile, metric, fieldcolumns, tagcolumns, delimiter)
    #    return True if 0 == len(datapoints) else return client.write_points(datapoints)
