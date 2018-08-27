#coding=utf-8
import json
import const as ct
import pandas as pd
from common import create_redis_obj
from cmysql import CMySQL
from cstock import CStock

if __name__ == "__main__":
    #redis = create_redis_obj()
    #redis.delete('ALL_TABLES')
    cmy = CMySQL(ct.DB_INFO)
    all_dbs = cmy.get_all_databases()
    for dbname in all_dbs:
        _dbname = dbname[1:]
        if _dbname.isnumeric():
            res = cmy.delete_db(dbname)
            print("delete %s, result:%s" % (dbname, res))

    #with open('/tmp/a', 'r') as f:
    #    slist = json.load(f)
    #old_stock = CStock(ct.OLD_DB_INFO, '000695')
    #for code in slist:
    #    #with open("/tmp/a", 'w') as f:
    #    #    json.dump(slist, f)
    #    old_stock.mysql_client.delete("%s_D" % code)
    #    old_stock.mysql_client.delete("%s_realtime" % code)
    #    old_stock.mysql_client.delete("%s_ticket" % code)
    #    print(code)
    #    #slist.append(code)
