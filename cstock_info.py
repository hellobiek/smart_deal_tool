#coding=utf-8
import time
import datetime
from datetime import datetime
import cmysql
import pandas as pd
import const as ct
import tushare as ts
import combination 
from log import getLogger
from common import trace_func

logger = getLogger(__name__)

class CStockInfo:
    @trace_func(log = logger)
    def __init__(self,dbinfo,table_name):
        self.table = table_name
        self.dbinfo = dbinfo
        self.mysql_client = cmysql.CMySQL(dbinfo)
        if not self.create(): raise Exception("create stock info table:%s failed" % self.table)

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(code varchar(10),\
                                              name varchar(10),\
                                              industry varchar(20),\
                                              cName varchar(1000),\
                                              area varchar(20),\
                                              pe float,\
                                              outstanding float,\
                                              totals float,\
                                              totalAssets float,\
                                              fixedAssets float,\
                                              liquidAssets float,\
                                              reserved float,\
                                              reservedPerShare float,\
                                              esp float,\
                                              bvps float,\
                                              pb float,\
                                              timeToMarket datetime,\
                                              timeLeaveMarket datetime,\
                                              undp float,\
                                              perundp float,\
                                              rev float,\
                                              profit float,\
                                              gpr float,\
                                              npr float,\
                                              limitUpNum int,\
                                              limitDownNum int,\
                                              holders int)' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)

    @trace_func(log = logger)
    def get_stock_data(self, code, date, dtype):
        table_name = "%s_D" % code if dtype == 'D' else "%s_realtime" % code
        if date is not None:
            sql = "select * from %s where date=\"%s\"" % (table_name, date)
            return self.mysql_client.get(sql)
        else:
            sql = "select * from %s" % table_name
            data = self.mysql_client.get(sql)
            #################################
            # test in real time to collect info
            #################################

    @trace_func(log = logger)
    def init(self):
        #get old stock data info
        old_df_all = self.mysql_client.get(ct.SQL % self.table)
        #get new stock data info
        df = ts.get_stock_basics()
        if not df.empty:
            df['limitUpNum'] = 0
            df['limitDownNum'] = 0
            df = df.reset_index(drop = False)
            if not old_df_all.empty:
                df_all = old_df_all.append(df, ignore_index=True)
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
    def get_classified_stocks(self, code_list = list()):
        df = self.mysql_client.get(ct.SQL % self.table)
        df = df[['code','name','timeToMarket','totals','outstanding','industry','area']]
        df['outstanding'] = df['outstanding'] * 1000000
        if len(code_list) > 0:
            df = df.ix[df.code.isin(code_list)]
        return df.sort_values('code').reset_index(drop=True)

    @trace_func(log = logger)
    def is_released(self, code_id, _date):
        time2Market = self.get(code = code_id, column = 'timeToMarket')
        if time2Market:
            t = time.strptime(str(time2Market), "%Y%m%d")
            y,m,d = t[0:3]
            time2Market = datetime(y,m,d)
            return (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0
        return False

    #@trace_func(log = logger)
    #def is_stock_exists(self, code_id):
    #    stock_info = self.mysql_client.get(ct.SQL % self.table)
    #    stock_info = stock_info[['code']]
    #    return True if len(stock_info[stock_info.code == code_id].index.tolist()) > 0 else False

    #@trace_func(log = logger)
    #def set_realtime_info(self):
    #    all_info = None
    #    start_index = 0
    #    stock_infos = self.get_classified_stocks()
    #    stock_nums = len(stock_infos)
    #    while start_index < stock_nums:
    #        end_index = stock_nums - 1 if start_index + 800 > stock_nums else start_index + 800 -1
    #        stock_codes = stock_infos['code'][start_index:end_index]
    #        _info = ts.get_realtime_quotes(stock_codes)
    #        all_info = _info if start_index == 0 else all_info.append(_info)
    #        start_index = end_index + 1
    #    all_info['limit_up_time'] = 0
    #    all_info['limit_down_time'] = 0
    #    all_info['outstanding'] = stock_infos['outstanding']
    #    all_info = all_info[(all_info['volume'].astype(float) > 0) & (all_info['outstanding'] > 0)]
    #    all_info['turnover'] = all_info['volume'].astype(float).divide(all_info['outstanding'])
    #    all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
    #    now_time = datetime.now().strftime('%H-%M-%S')
    #    all_info[all_info["p_change"]>9.9]['limit_up_time'] = now_time
    #    all_info[all_info["p_change"]<-9.9]['limit_down_time'] = now_time
    #    self.mysql_client.set(all_info, table)
