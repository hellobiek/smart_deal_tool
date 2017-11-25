#encoding=utf-8
import pandas as pd
from time import sleep
from const import SHORT_SLEEP_TIME

def set(engine, data_frame, table):
    data_frame.to_sql(table,engine,if_exists='replace',index=False)
    sleep(SHORT_SLEEP_TIME)

def get(engine, sql):
    return pd.read_sql_query(sql, engine)

def get_hist_data(engine, table, date = None):
    sql = ""
    if table.isdigit():
        if date is not None:
            sql = "select * from `%s` where date=\"%s\"" %(table, date)
        else:
            sql = "select * from `%s`" % table
    else:
        if date is not None:
            sql = "select * from %s where date=\"%s\"" %(table, date)
        else:
            sql = "select * from %s" % table
    return pd.read_sql_query(sql, engine)
