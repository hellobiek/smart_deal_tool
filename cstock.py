#coding=utf-8
import time
import datetime
from datetime import datetime
import const as ct
import pandas as pd
import tushare as ts
import cstock_info as cs_info
from cmysql import CMySQL
from log import getLogger
from common import trace_func,is_trading_time,_fprint

logger = getLogger(__name__)

class CStock:
    #@trace_func(log = logger)
    def __init__(self, dbinfo, code, info_table):
        self.code = code
        self.data_type_dict = {'D':"%s_D" % code}
        self.realtime_table = "%s_realtime" % self.code
        self.info_table = info_table
        self.info_client = cs_info.CStockInfo(dbinfo, self.info_table)
        self.mysql_client = CMySQL(dbinfo)
        self.name = self.get('name')
        if not self.create(): raise Exception("create stock %s table failed" % self.code)

    #@trace_func(log = logger)
    def is_subnew(self, time2Market = None, timeLimit = 365):
        if time2Market == '0': return False #for stock has not been in market
        if time2Market == None: time2Market = self.get('timeToMarket')
        t = time.strptime(time2Market, "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)
        return True if (datetime.today()-time2Market).days < timeLimit else False

    #@trace_func(log = logger)
    def create_static(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(date varchar(10),\
                                                    open float,\
                                                    high float,\
                                                    close float,\
                                                    low float,\
                                                    volume float)' % table_name
                if not self.mysql_client.create(sql): return False
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
                                              b1_v int,\
                                              b1_p float,\
                                              b2_v int,\
                                              b2_p float,\
                                              b3_v int,\
                                              b3_p float,\
                                              b4_v int,\
                                              b4_p float,\
                                              b5_v int,\
                                              b5_p float,\
                                              a1_v int,\
                                              a1_p float,\
                                              a2_v int,\
                                              a2_p float,\
                                              a3_v int,\
                                              a3_p float,\
                                              a4_v int,\
                                              a4_p float,\
                                              a5_v int,\
                                              a5_p float,\
                                              time varchar(20),\
                                              turnover float,\
                                              p_change float,\
                                              outstanding float,\
                                              limit_dowm_time varchar(20),\
                                              limit_up_time varchar(20))' % self.realtime_table
        return True if self.realtime_table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)

    #@trace_func(log = logger)
    def get(self, attribute):
        sql = "select %s from %s where code = %s" % (attribute, self.info_table, self.code)
        _info = self.mysql_client.get(sql)
        return _info.loc[0, attribute]

    #@trace_func(log = logger)
    def create(self):
        return self.create_static() and self.create_realtime()

    #@trace_func(log = logger)
    def init(self):
        old_data = self.get_k_data()
        _today = datetime.now().strftime('%Y-%m-%d')
        for d_type,d_table_name in self.data_type_dict.items():
            new_data = ts.get_k_data(self.code,ktype=d_type,start=ct.START_DATE,end=_today,retry_count = ct.RETRY_TIMES)
            if not new_data.empty:
                data = old_data.append(new_data)
                data = data.drop_duplicates(subset = 'date').reset_index(drop=True)
                self.mysql_client.set(data, d_table_name)

    #@trace_func(log = logger)
    def run(self, evt):
        data = evt.get()
        _info = data[data.name == self.name]
        self.mysql_client.set(_info, self.realtime_table, method = ct.APPEND)

    ##collect realtime data
    #@trace_func(log = logger)
    #def run(self, sem, evt, data):
    #    sem.acquire()
    #    if is_trading_time():
    #        _info = ts.get_realtime_quotes(self.code)
    #        if _info is not None:
    #            _info['limit_up_time'] = 0
    #            _info['limit_down_time'] = 0
    #            _info['outstanding'] = self.get('outstanding')
    #            _info['turnover'] = _info['volume'].astype(float).divide(_info['outstanding'])
    #            _info['p_change'] = 100 * (_info['price'].astype(float) - _info['pre_close'].astype(float)).divide(_info['pre_close'].astype(float))
    #            now_time = datetime.now().strftime('%H-%M-%S')
    #            _info['limit_up_time'] = now_time if _info['p_change'][0] > 9.9 else 0
    #            _info['limit_down_time'] = now_time if _info['p_change'][0] < -9.9 else 0
    #            sql = "select * from %s" % self.realtime_table
    #            old_data = self.mysql_client.get(sql)
    #            _info = old_data.append(_info)
    #            self.mysql_client.set(_info, self.realtime_table)
    #    sem.release()

    #@trace_func(log = logger)
    def get_k_data(self, date = None, dtype = 'D'):
        table_name = self.data_type_dict[dtype] 
        if date is not None:
            sql = "select * from %s where date=\"%s\"" %(table_name, date)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    #@trace_func(log = logger)
    def is_after_release(self, code_id, _date):
        time2Market = self.get('timeToMarket')
        t = time.strptime(str(time2Market), "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)
        return (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0
