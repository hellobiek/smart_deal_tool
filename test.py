#coding=utf-8
import redis
import _pickle
import pandas as pd
import tushare as ts
import const as ct
from cmysql import CMySQL
from common import get_redis_name

ALL_TABLES = 'all_tables'
#host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=False)
r = redis.StrictRedis(connection_pool=pool)
all_tables = r.smembers(ALL_TABLES)
#for table in ['300318_ticket', '300308_ticket', '300328_ticket', '300338_ticket', '300348_ticket']:
for table in all_tables:
    if str(table).find("_ticket") != -1:
        r.srem('all_tables', table)

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
