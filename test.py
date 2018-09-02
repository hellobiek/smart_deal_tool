#coding=utf-8
import os
import redis
import const as ct

pool = redis.ConnectionPool(host=ct.REDIS_HOST, port=ct.REDIS_PORT, decode_responses=False)
r = redis.StrictRedis(connection_pool=pool)
df_byte = r.smembers('all_existed_stocks')
print(len(df_byte))

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
#sql = 'select * from industry where date = "2018-08-24"'
#df = cm.get(sql)
#print(df)
