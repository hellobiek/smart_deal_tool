#coding=utf-8
import redis
import _pickle
import pandas as pd
import tushare as ts
import const as ct
from cmysql import CMySQL
from common import get_redis_name

#host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=False)
r = redis.StrictRedis(connection_pool=pool)
print(r.keys())
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
