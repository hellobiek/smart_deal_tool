#coding=utf-8
import redis
import _pickle
import pandas as pd
import tushare as ts
import const as ct
from cmysql import CMySQL
from common import get_redis_name

ALL_TABLES = 'all_tables'
pool = redis.ConnectionPool(host=ct.REDIS_HOST, port=ct.REDIS_PORT, decode_responses=False)
r = redis.StrictRedis(connection_pool=pool)
df_byte = r.get(ct.ANIMATION_INFO)
df = _pickle.loads(df_byte)
print(df)

#all_tables = r.smembers(ALL_TABLES)
##for table in ['300318_ticket', '300308_ticket', '300328_ticket', '300338_ticket', '300348_ticket']:
#for table in all_tables:
#    if str(table).find("_ticket") != -1:
#        r.srem('all_tables', table)

#if table.starts_with("300") and 
#'300%8_ticket%'

#print(r.exists(get_redis_name('000762')))
#print(r.get(get_redis_name('700017')))

#count = 0
#while True:
#    print(ts.get_realtime_quotes(['600848','000980','000981']))
#    count+=1
#    print(count)

#cm = CMySQL(ct.DB_INFO)
#print(cm.get_all_tables())
#print(len(cm.get_all_tables()))
