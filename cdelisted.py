#coding=utf-8
from pandas import DataFrame
import time
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
from cmysql import CMySQL
from common import trace_func, _fprint

logger = getLogger(__name__)

class CDelisted:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        self.dbinfo = dbinfo
        self.table = table_name
        self.mysql_client = CMySQL(dbinfo)
        if not self.create(): raise Exception("create delisted info table:%s failed" % self.table)

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(code varchar(6),\
                                              name varchar(8),\
                                              oDate varchar(10),\
                                              tDate varchar(10))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)
    
    @trace_func(log = logger)
    def init(self):
        #get old delisted data info
        old_df_all = self.mysql_client.get(ct.SQL % self.table)
        if old_df_all is not None:
            #get new delisted data info
            df_terminated = ts.get_terminated()
            if not df_terminated.empty:
                df = df_terminated
            df_suspended = ts.get_suspended()
            if not df_suspended.empty:
                df = df.append(df_suspended)
            if not df.empty:
                df = df.reset_index(drop = True)
                if not old_df_all.empty:
                    old_df_all = old_df_all.reset_index(drop = True)
                    df_all = old_df_all.append(df)
                    df = df_all.drop_duplicates(subset = 'code')
            self.mysql_client.set(df,self.table)

    @trace_func(log = logger)
    def get(self, code = None, column = None):
        if code is None:return self.mysql_client.get(ct.SQL % self.table)
        if column is None:
            sql = "select * from %s where code=\"%s\"" % (self.table, code)
            return self.mysql_client.get(sql)
        else:
            sql = "select %s from %s where code=\"%s\"" % (column, self.table, code)
            df = self.mysql_client.get(sql)
            return df[column][0]

    @trace_func(log = logger)
    def is_dlisted(self, code_id, _date):
        if code_id not in self.get_list(): return False
        terminatedTime = self.get(code = code_id, column = 'tDate')
        if terminatedTime == '-': return True
        if terminatedTime:
            t = time.strptime(terminatedTime, "%Y-%m-%d")
            y,m,d = t[0:3]
            terminatedTime = datetime(y,m,d)
            return (datetime.strptime(_date, "%Y-%m-%d") - terminatedTime).days > 0
        return False

    @trace_func(log = logger)
    def get_list(self):
        old_df_all = self.mysql_client.get(ct.SQL % self.table)
        return old_df_all['code'].tolist()

if __name__ == '__main__':
    cdlist = CDelisted(ct.DB_INFO, 'delisted')
    print(cdlist.init())
