# -*- coding: utf-8 -*-
import json
import _pickle
import cmysql
import const as ct
import pandas as pd
from pathlib import Path
from cindex import CIndex
from pandas import DataFrame
from base.clog import getLogger
from common import create_redis_obj, concurrent_run
logger = getLogger(__name__)
TONG_DA_XIN_INDUSTRY_PATH = "/tongdaxin/incon.dat"
TONG_DA_XIN_CODE_FILE = "/data/tdx/base/stocks.csv"
TONG_DA_XIN_CODE_PATH = "/tongdaxin/T0002/hq_cache/tdxhy.cfg"
class IndustryInfo(object):
    data = None
    def __init__(self, name = "TDX", dbinfo = ct.DB_INFO, redis_host = None, without_init = True):
        self.name = name
        self.table = ct.INDUSTRY_INFO if self.name == 'TDX' else ct.SW_INDUSTRY_INFO
        self.redis = create_redis_obj(host = 'redis-proxy-container', port = 6579) if redis_host is None else create_redis_obj(host = redis_host, port = 6579)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        if not without_init:
            if not self.init(): raise Exception("init combination info failed")
        IndustryInfo.data = self.get_data()

    def get_data(self):
        df_byte = self.redis.get(self.table)
        if df_byte is None:
            raise Exception("stock data in redis is None")
        return pd.DataFrame() if df_byte is None else _pickle.loads(df_byte)

    def init(self):
        if self.name == 'TDX':
            df = self.get_tdx_industry()
        else:
            df = self.get_sw_industry()
        df = df.reset_index(drop = True)
        self.redis.set(self.table, _pickle.dumps(df, 2))
        return True

    def create_obj(self, code):
        try:
            CIndex(code, should_create_influxdb = True, should_create_mysqldb = True)
            return (code, True)
        except Exception as e:
            return (code, False)

    def update(self):
        if self.init():
            df = self.get_data()
            return concurrent_run(self.create_obj, df.code.tolist(), num = 30)
        return False

    def get_industry_name_dict_from_tongdaxin(self, fname):
        industry_dict = dict()
        hy_name = '#TDXNHY' if self.name == 'TDX' else '#SWHY'
        with open(fname, "rb") as f: data = f.read()
        info_list = data.decode("gbk").split('######\r\n')
        for info in info_list:
            xlist = info.split('\r\n')
            if xlist[0] == hy_name:
                zinfo = xlist[1:len(xlist)-1]
        for z in zinfo:
            x = z.split('|')
            industry_dict[x[0]] = x[1]
        return industry_dict

    @staticmethod
    def is_stock(code, market):
        if code.startswith("00") and market == '0':
            return True
        elif code.startswith("30") and market == '0':
            return True
        elif code.startswith("6") and market == '1':
            return True
        else:
            return False

    def get_industry_code_dict_from_tongdaxin(self, fname):
        industry_dict = dict()
        hy_index = 2 if self.name == 'TDX' else 3
        with open(fname, "rb") as f: data = f.read()
        str_list = data.decode("utf-8").split('\r\n')
        for x in str_list:
            info_list = x.split('|')
            if len(info_list) == 5:
                code = info_list[1]
                market = info_list[0]
                if IndustryInfo.is_stock(code, market):
                    industry = info_list[hy_index]
                    if industry == 'T00' or industry == '0': continue
                    if industry not in industry_dict: industry_dict[industry] = list()
                    industry_dict[industry].append(code)
        for key in industry_dict:
            industry_dict[key] = json.dumps(industry_dict[key])
        return industry_dict

    def get_tdx_industry_code(self, fname = TONG_DA_XIN_CODE_FILE):
        data = pd.read_csv(TONG_DA_XIN_CODE_FILE, sep = ',', dtype = {'code' : str, 'market': int, 'name': str})
        data = data[['code', 'name']]
        data = data[data.code.str.startswith('880')]
        data = data.reset_index(drop = True)
        return data

    def get_sw_industry(self):
        '''获取申万行业'''
        industry_code_dict = self.get_industry_code_dict_from_tongdaxin(TONG_DA_XIN_CODE_PATH)
        industry_code_df = pd.DataFrame(list(industry_code_dict.items()), columns=['code', 'content'])
        industry_name_dict = self.get_industry_name_dict_from_tongdaxin(TONG_DA_XIN_INDUSTRY_PATH)
        industry_name_df = pd.DataFrame(list(industry_name_dict.items()), columns=['code', 'name'])
        df = pd.merge(industry_code_df, industry_name_df, how='left', on=['code'])
        df = df.reset_index(drop = True)
        return df[['code', 'name', 'content']]

    def get_tdx_industry(self):
        '''获取通达信行业'''
        industry_tdx_df = self.get_tdx_industry_code()
        industry_code_dict = self.get_industry_code_dict_from_tongdaxin(TONG_DA_XIN_CODE_PATH)
        industry_name_dict = self.get_industry_name_dict_from_tongdaxin(TONG_DA_XIN_INDUSTRY_PATH)
        industry_name_dict['T020604'] = '其它建材'
        name_list = [industry_name_dict[key] for key in industry_code_dict]
        data = {'name':name_list, 'content':list(industry_code_dict.values())}
        df_new = pd.DataFrame.from_dict(data)
        df = pd.merge(df_new, industry_tdx_df, how='left', on=['name'])
        df = df.drop_duplicates(['name'], keep='first')
        df = df.reset_index(drop = True)
        return df[['code', 'name', 'content']]

if __name__ == '__main__':
    ci = IndustryInfo(dbinfo = ct.DB_INFO, redis_host = None, without_init = False)
    data = ci.get_data()
