#coding=utf-8
import time
import cmysql
import _pickle
import datetime
import combination 
from datetime import datetime
import pandas as pd
import const as ct
import tushare as ts
from log import getLogger
from common import trace_func, create_redis_obj, df_delta

logger = getLogger(__name__)

class CStockInfo:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        self.table = table_name
        self.trigger = ct.SYNCSTOCK2REDIS
        self.mysql_client = cmysql.CMySQL(dbinfo)
        self.redis = create_redis_obj()
        if not self.create(): raise Exception("create stock info table:%s failed" % self.table)
        if not self.init(): raise Exception("init stock info table failed")
        if not self.register(): raise Exception("create trigger info table:%s failed" % self.trigger)

    @trace_func(log = logger)
    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s',json_object('code',NEW.code,'name',NEW.name,'industry',NEW.industry,'area',NEW.area,'pe',NEW.pe,'outstanding',NEW.outstanding,'totals',NEW.totals,'totalAssets',NEW.totalAssets,'fixedAssets',NEW.fixedAssets,'liquidAssets',NEW.liquidAssets,'reserved',NEW.reserved,'reservedPerShare',NEW.reservedPerShare,'esp',NEW.esp,'bvps',NEW.bvps,'pb',NEW.pb,'timeToMarket',NEW.timeToMarket,'undp',NEW.undp,'perundp',NEW.perundp,'rev',NEW.rev,'profit',NEW.profit,'gpr',NEW.gpr,'npr',NEW.npr,'limitUpNum',NEW.limitUpNum,'limitDownNum',NEW.limitDownNum,'holders',NEW.holders));" % (self.trigger,self.table,self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

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
                                              timeToMarket varchar(20),\
                                              timeLeaveMarket varchar(20),\
                                              undp float,\
                                              perundp float,\
                                              rev float,\
                                              profit float,\
                                              gpr float,\
                                              npr float,\
                                              limitUpNum int,\
                                              limitDownNum int,\
                                              holders int)' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    @trace_func(log = logger)
    def init(self):
        df = ts.get_stock_basics()
        if df is None: return False 
        df = df.reset_index(drop = False)
        df['limitUpNum'] = 0
        df['limitDownNum'] = 0
        return self.redis.set(ct.STOCK_INFO, _pickle.dumps(df, 2))

    @trace_func(log = logger)
    def get(self, code = None, column = None):
        df_byte = self.redis.get(ct.STOCK_INFO)
        if df_byte is None: return pd.DataFrame()
        df = _pickle.loads(df_byte)
        if code is None: return df
        if column is None:
            return df.loc[df.code == code]
        else:
            return df.loc[df.code == code][column].values[0]

    @trace_func(log = logger)
    def get_classified_stocks(self, code_list = list()):
        df = self.get()
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

    #@trace_func(log = logger)
    #def get_stock_data(self, code, date, dtype):
    #    table_name = "%s_D" % code if dtype == 'D' else "%s_realtime" % code
    #    if date is not None:
    #        sql = "select * from %s where date=\"%s\"" % (table_name, date)
    #        return self.mysql_client.get(sql)
    #    else:
    #        sql = "select * from %s" % table_name
    #        data = self.mysql_client.get(sql)
    #        #################################
    #        # test in real time to collect info
    #        #################################

if __name__ == '__main__':
    CStockInfo(ct.DB_INFO, ct.STOCK_INFO_TABLE)
