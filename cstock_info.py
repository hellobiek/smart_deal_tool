#coding=utf-8
import os
import time
import json
import cmysql
import _pickle
import datetime
import pandas as pd
import const as ct
import tushare as ts
from cstock import CStock
from datetime import datetime
from base.clog import getLogger
from industry_info import IndustryInfo
from common import create_redis_obj, concurrent_run, smart_get
logger = getLogger(__name__)
class CStockInfo(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, stocks_dir = '/data/tdx/history/days', stock_path = '/data/tdx/base/stocks.csv'):
        self.table = ct.STOCK_INFO_TABLE
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        self.mysql_dbs = self.mysql_client.get_all_databases()
        self.stocks_dir = stocks_dir
        self.stock_path = stock_path
        self.industry_info = IndustryInfo.get_industry()
        #self.trigger = ct.SYNCSTOCK2REDIS
        #if not self.create(): raise Exception("create stock info table:%s failed" % self.table)
        #if not self.register(): raise Exception("create trigger info table:%s failed" % self.trigger)

    @staticmethod
    def get(code = None, column = None, redis = None, redis_host = None):
        if redis is None: redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        df_byte = redis.get(ct.STOCK_INFO)
        if df_byte is None: return pd.DataFrame()
        df = _pickle.loads(df_byte)
        if code is None: return df
        if column is None:
            return df.loc[df.code == code]
        else:
            return df.loc[df.code == code][column].values[0]

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
        df = self.get_basics()
        if df is None: return False
        df = df.reset_index(drop = False)
        return self.redis.set(ct.STOCK_INFO, _pickle.dumps(df, 2))

    def update(self):
        if self.init():
            df = self.get(redis = self.redis)
            return concurrent_run(self.create_stock_obj, df.code.tolist(), num = 10)
        return False 

    def get_basics(self):
        def func(code):
            industry = self.get_industry(code)
            timeToMarket = self.get_time_to_market(code)
            return industry, timeToMarket
        base_df = self.get_base_stock_info()
        base_df['industry'], base_df['timeToMarket'] = zip(*base_df.apply(lambda base_df: func(base_df['code']), axis = 1))
        base_df = base_df.loc[base_df.timeToMarket != 0]
        base_df = base_df.reset_index(drop = True)
        return base_df

    def get_time_to_market(self, code): 
        """获取沪深股股票上市时间"""
        file_name = "%s%s.csv" % (CStock.get_pre_str(code), code)
        file_path = os.path.join(self.stocks_dir, file_name)
        if not os.path.exists(file_path): return 0
        with open(file_path, 'r') as f:
            lines = f.readlines()
            return int(lines[1].split(',')[2])

    def get_industry(self, code):
        """获取沪深股股票通达信行业信息"""
        rdf = self.industry_info[self.industry_info.content.str.contains(code)]
        return rdf['name'].values[0] if not rdf.empty else None

    def get_base_stock_info(self):
        """获取沪深股票列表"""
        try:
            base_df = pd.read_csv(self.stock_path, header=0)
            base_df['name'] = base_df['name'].map(lambda x: str(x))
            base_df['code'] = base_df['code'].map(lambda x: str(x).zfill(6))
            filter_df = base_df[((base_df['code'].str.startswith("00")) & (base_df['market'] == 0)) |
                                ((base_df['code'].str.startswith("30")) & (base_df['market'] == 0)) |
                                ((base_df['code'].str.startswith("68")) & (base_df['market'] == 1)) |
                                ((base_df['code'].str.startswith("60")) & (base_df['market'] == 1))]
            filter_df = filter_df[['code', 'name']]
            filter_df = filter_df.reset_index(drop = True)
            return filter_df
        except Exception as e:
            logger.error(e)
            return pd.DataFrame()

    def get_classified_stocks(self, code_list = list()):
        df = self.get()
        df = df[['code','name','timeToMarket', 'industry']]
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
    cs_info = CStockInfo(ct.DB_INFO)
    #mdate = cs_info.get_time_to_market("600902")
    #info = cs_info.get_industry('601318')
    #cs_info.get_base_stock_info()
    #df = cs_info.get_basics()
