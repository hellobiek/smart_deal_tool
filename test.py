#coding=utf-8
import redis
import _pickle
import pandas as pd
import tushare as ts
import const as ct
from cmysql import CMySQL
from cinfluxdb import CInflux 

influx_client = CInflux(ct.IN_DB_INFO, "123456")
df_list = influx_client.list_all_databases()
for db_item in df_list:
    db_name = db_item['name']
    if db_name.startswith("s"): 
        influx_client.delete(db_name)
        print("delete %s failed" % db_name)

#pool = redis.ConnectionPool(host=ct.REDIS_HOST, port=ct.REDIS_PORT, decode_responses=False)
#r = redis.StrictRedis(connection_pool=pool)
#df_byte = r.get(ct.COMBINATION_INFO)
#df = _pickle.loads(df_byte)

#all_tables = r.smembers(ALL_TABLES)
##for table in ['300318_ticket', '300308_ticket', '300328_ticket', '300338_ticket', '300348_ticket']:
#for table in all_tables:
#    if str(table).find("_ticket") != -1:
#        r.srem('all_tables', table)

#if table.starts_with("300") and 
#'300%8_ticket%'

#count = 0
#while True:
#    print(ts.get_realtime_quotes(['600848','000980','000981']))
#    count+=1
#    print(count)

#cm = CMySQL(ct.DB_INFO)
#print(cm.get_all_tables())
#print(len(cm.get_all_tables()))
