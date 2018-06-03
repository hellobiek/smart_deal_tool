#coding=utf-8
import cmysql
import datetime
from datetime import datetime
import const as ct
import tushare as ts
import combination_info as cm_info 
from log import getLogger
from common import trace_func

logger = getLogger(__name__)

class Combination:
    #@trace_func(log = logger)
    def __init__(self, dbinfo, code, info_table):
        self.code = code
        self.info_table = info_table
        self.data_type_dict = {'D':"c%s_D" % code}
        self.realtime_table = "c%s_realtime" % code
        self.info_client = cm_info.CombinationInfo(dbinfo, info_table)
        #exectued before self.info_table and self.mysql_client both initted
        self.mysql_client = cmysql.CMySQL(dbinfo)
        self.name = self.get('name')
        if not self.create(): raise Exception("create combination table failed")

    #@trace_func(log = logger)
    def create_static(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(date varchar(10),\
                                                        code varchar(10),\
                                                        open float,\
                                                        high float,\
                                                        close float,\
                                                        low float,\
                                                        volume float)' % table_name
                if not  self.mysql_client.create(sql): return False
        return True

    #@trace_func(log = logger)
    def create_realtime(self):
        sql = 'create table if not exists %s(date varchar(25),\
                                              name varchar(20),\
                                              code varchar(10),\
                                              open float,\
                                              pre_close float,\
                                              price float,\
                                              high float,\
                                              low float,\
                                              bid float,\
                                              ask float,\
                                              volume float,\
                                              amount float,\
                                              time varchar(20),\
                                              p_change float,\
                                              outstanding float)' % self.realtime_table
        return True if self.realtime_table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)

    #@trace_func(log = logger)
    def create(self):
        return self.create_static() and self.create_realtime()

    #@trace_func(log = logger)
    def init(self):
        _today = datetime.now().strftime('%Y-%m-%d')
        for data_type, data_table in self.data_type_dict.items():
            old_data = self.get_k_data()
            new_data = ts.get_k_data(self.code,index=True,ktype=data_type,start=ct.START_DATE,end=_today,retry_count = ct.RETRY_TIMES)
            if not new_data.empty:
                data = old_data.append(new_data)
                data = data.drop_duplicates(subset = 'date').reset_index(drop = True)
                self.mysql_client.set(data, data_table)

    #@trace_func(log = logger)
    def get_k_data(self, date = None, dtype = 'D'):
        table_name = self.data_type_dict[dtype]
        if date is not None:
            sql = "select * from %s where date=\"%s\"" %(table_name, date)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    #@trace_func(log = logger)
    def get(self, attribute):
        sql = "select %s from %s where code = %s" % (attribute, self.info_table, self.code)
        _info = self.mysql_client.get(sql)
        return _info.loc[0, attribute]

    #@trace_func(log = logger)
    def run(self):
        all_info = ts.get_realtime_quotes(self.name)
        if all_info is not None:
            all_info = all_info[['name','code','open','pre_close','price','high','low','volume','date','time','amount']]
            all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
            self.mysql_client.set(all_info, self.realtime_table, method = ct.APPEND)
