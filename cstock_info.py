# -*- coding: utf-8 -*-
import os
import time
import cmysql
import _pickle
import datetime
import const as ct
import pandas as pd
from datetime import datetime
from base.clog import getLogger
from industry_info import IndustryInfo
from common import get_pre_str, create_redis_obj
logger = getLogger(__name__)
class CStockInfo(object):
    data = None
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, stocks_dir = ct.STOCKS_DIR, base_stock_path = ct.BASE_STOCK_PATH, without_init = True):
        self.dbinfo = dbinfo
        self.stocks_dir = stocks_dir
        self.redis_host = redis_host
        self.table = ct.STOCK_INFO_TABLE
        self.base_stock_path = base_stock_path
        self.redis = create_redis_obj(host = 'redis-proxy-container', port = 6579) if redis_host is None else create_redis_obj(host = redis_host, port = 6579)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        #self.trigger = ct.SYNCSTOCK2REDIS
        #if not self.create(): raise Exception("create stock info table:%s failed" % self.table)
        #if not self.register(): raise Exception("create trigger info table:%s failed" % self.trigger)
        if not without_init:
            if not self.init(): raise Exception("stock info init failed")
        CStockInfo.data = self.get_data()

    def get_data(self):
        df_byte = self.redis.get(ct.STOCK_INFO)
        if df_byte is None:
            raise Exception("stock data in redis is None")
        df = _pickle.loads(df_byte)
        return df 

    def get(self, code = None, column = None):
        if code is None: return CStockInfo.data
        if column is None:
            return CStockInfo.data.loc[CStockInfo.data.code == code]
        else:
            return CStockInfo.data.loc[CStockInfo.data.code == code][column].values[0]

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s',json_object('code',NEW.code,'name',NEW.name,'industry',NEW.industry,'area',NEW.area,'pe',NEW.pe,'outstanding',NEW.outstanding,'totals',NEW.totals,'totalAssets',NEW.totalAssets,'fixedAssets',NEW.fixedAssets,'liquidAssets',NEW.liquidAssets,'reserved',NEW.reserved,'reservedPerShare',NEW.reservedPerShare,'esp',NEW.esp,'bvps',NEW.bvps,'pb',NEW.pb,'timeToMarket',NEW.timeToMarket,'undp',NEW.undp,'perundp',NEW.perundp,'rev',NEW.rev,'profit',NEW.profit,'gpr',NEW.gpr,'npr',NEW.npr,'limitUpNum',NEW.limitUpNum,'limitDownNum',NEW.limitDownNum,'holders',NEW.holders));" % (self.trigger, self.table, self.trigger)
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

    def init(self):
        df = self.get_basics()
        if df is None: return False
        df = df.reset_index(drop = True)
        CStockInfo.data = df
        return self.redis.set(ct.STOCK_INFO, _pickle.dumps(df, 2))

    def get_basics(self):
        def func(code, tdx_industry_info, sw_industry_info):
            industry = self.get_industry(code, tdx_industry_info)
            sw_industry = self.get_industry(code, sw_industry_info)
            timeToMarket = self.get_time_to_market(code)
            return industry, sw_industry, timeToMarket
        base_df = self.get_base_stock_info()
        tdx_industry_info = IndustryInfo("TDX", self.dbinfo, self.redis_host).get_data()
        sw_industry_info = IndustryInfo("SW", self.dbinfo, self.redis_host).get_data()
        base_df['industry'], base_df['sw_industry'], base_df['timeToMarket'] = zip(*base_df.apply(lambda base_df: func(base_df['code'], tdx_industry_info, sw_industry_info), axis = 1))
        base_df = base_df.loc[base_df.timeToMarket != 0]
        base_df = base_df.reset_index(drop = True)
        return base_df

    def get_time_to_market(self, code): 
        """获取沪深股股票上市时间"""
        file_name = "%s%s.csv" % (get_pre_str(code), code)
        file_path = os.path.join(self.stocks_dir, file_name)
        if not os.path.exists(file_path): return 0
        with open(file_path, 'r') as f:
            lines = f.readlines()
            return int(lines[1].split(',')[2])

    def get_industry(self, code, rdf):
        rdf = rdf[rdf.content.str.contains(code)]
        return rdf['name'].values[0] if not rdf.empty else None

    def get_base_stock_info(self):
        """获取沪深股票列表"""
        try:
            base_df = pd.read_csv(self.base_stock_path, header=0)
            base_df['name'] = base_df['name'].map(lambda x: str(x))
            base_df['code'] = base_df['code'].map(lambda x: str(x).zfill(6))
            filter_df = base_df[((base_df['code'].str.startswith("00")) & (base_df['market'] == 0)) |
                                ((base_df['code'].str.startswith("30")) & (base_df['market'] == 0)) |
                                ((base_df['code'].str.startswith("6"))  & (base_df['market'] == 1))]
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
    cs_info = CStockInfo(ct.DB_INFO, without_init = False)
    #mdate = cs_info.get_time_to_market("600902")
    #info = cs_info.get_industry('601318')
    #cs_info.get_base_stock_info()
    #df = cs_info.get_basics()
