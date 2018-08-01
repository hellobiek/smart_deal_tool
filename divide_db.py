#coding=utf-8
import json
import const as ct
import pandas as pd
import tushare as ts
from common import create_redis_obj
from cnmysql import CNMySQL
from cstock import CStock
from cnstock import CNStock

if __name__ == "__main__":
    #redis = create_redis_obj()
    #redis.delete('ALL_TABLES')
    cmy = CNMySQL(ct.DB_INFO)
    all_tables = cmy.get_all_tables()
    all_tables = [_table.split('_')[0] for _table in all_tables]
    all_tables = list(set(all_tables))
    all_dbs = cmy.get_all_databases()
    for _table in all_tables:
        dbname = "s%s" % _table
        if _table.isnumeric() and dbname not in all_dbs:
            print(cmy.create_db(dbname))
    with open('/tmp/a', 'r') as f:
        slist = json.load(f)
    for code in all_tables:
        if code.isnumeric() and code not in slist:
            with open("/tmp/a", 'w') as f:
                json.dump(slist, f)
            print(code)
            old_stock = CStock(ct.OLD_DB_INFO, code)
            new_stock = CNStock(ct.DB_INFO, code)
            old_d_df = old_stock.get_k_data()
            new_stock.mysql_client.set(old_d_df, table = "day")
            slist.append(code)
