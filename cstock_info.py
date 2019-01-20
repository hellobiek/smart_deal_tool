#coding=utf-8
import time
import cmysql
import _pickle
import datetime
from datetime import datetime
import pandas as pd
import const as ct
import tushare as ts
from cstock import CStock 
from log import getLogger
from common import create_redis_obj, concurrent_run, smart_get
logger = getLogger(__name__)

class CStockInfo:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.table = ct.STOCK_INFO_TABLE
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        self.mysql_dbs = self.mysql_client.get_all_databases()
        #self.trigger = ct.SYNCSTOCK2REDIS
        #if not self.create(): raise Exception("create stock info table:%s failed" % self.table)
        if not self.init():
            raise Exception("init stock info table failed")
        #if not self.register(): raise Exception("create trigger info table:%s failed" % self.trigger)

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s',json_object('code',NEW.code,'name',NEW.name,'industry',NEW.industry,'area',NEW.area,'pe',NEW.pe,'outstanding',NEW.outstanding,'totals',NEW.totals,'totalAssets',NEW.totalAssets,'fixedAssets',NEW.fixedAssets,'liquidAssets',NEW.liquidAssets,'reserved',NEW.reserved,'reservedPerShare',NEW.reservedPerShare,'esp',NEW.esp,'bvps',NEW.bvps,'pb',NEW.pb,'timeToMarket',NEW.timeToMarket,'undp',NEW.undp,'perundp',NEW.perundp,'rev',NEW.rev,'profit',NEW.profit,'gpr',NEW.gpr,'npr',NEW.npr,'limitUpNum',NEW.limitUpNum,'limitDownNum',NEW.limitDownNum,'holders',NEW.holders));" % (self.trigger,self.table,self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    def create(self):
        sql = 'create table if not exists %s(code varchar(10) not null,\
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
                                              holders int,\
                                              PRIMARY KEY (code))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    def create_stock_obj(self, code):
        try:
            CStock(code, should_create_influxdb = True, should_create_mysqldb = True)
            return (code, True)
        except Exception as e:
            logger.info(e)
            return (code, False)

    def init(self):
        df = smart_get(ts.get_stock_basics)
        if df is None: return False
        df = df[~df.index.isin(ct.BLACK_LIST)]
        df = df.reset_index(drop = False)
        return self.redis.set(ct.STOCK_INFO, _pickle.dumps(df, 2))

    def update(self):
        if self.init():
            df = self.get(redis = self.redis)
            return concurrent_run(self.create_stock_obj, df.code.tolist(), num = 10)
        return False 

    @staticmethod
    def get(code = None, column = None, redis = None):
        redis = create_redis_obj() if redis is None else redis
        df_byte = redis.get(ct.STOCK_INFO)
        if df_byte is None: return pd.DataFrame()
        df = _pickle.loads(df_byte)
        if code is None: return df
        if column is None:
            return df.loc[df.code == code]
        else:
            return df.loc[df.code == code][column].values[0]

    def get_classified_stocks(self, code_list = list()):
        df = self.get()
        df = df[['code','name','timeToMarket','totals','outstanding','industry','area']]
        df['outstanding'] = df['outstanding'] * 1000000
        if len(code_list) > 0:
            df = df.ix[df.code.isin(code_list)]
        return df.sort_values('code').reset_index(drop=True)

    def is_released(self, code_id, _date):
        time2Market = self.get(code = code_id, column = 'timeToMarket')
        if time2Market:
            t = time.strptime(str(time2Market), "%Y%m%d")
            y,m,d = t[0:3]
            time2Market = datetime(y,m,d)
            return (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0
        return False

if __name__ == '__main__':
    CStockInfo(ct.DB_INFO, ct.STOCK_INFO_TABLE)
