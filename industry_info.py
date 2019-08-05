# coding=utf-8
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
CSI_INDUSRT_DIR = "/data/crawler/china_security_industry_valuation/stock"
class IndustryInfo(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.table = ct.INDUSTRY_INFO
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        if not self.init(): raise Exception("init combination table failed")

    def init(self):
        new_df = DataFrame()
        new_self_defined_df = self.get_industry()
        new_self_defined_df['best'] = '0'
        new_df = new_df.append(new_self_defined_df)
        new_df = new_df.reset_index(drop = True)
        self.redis.set(ct.INDUSTRY_INFO, _pickle.dumps(new_df, 2))
        return True

    def create_obj(self, code):
        try:
            CIndex(code, should_create_influxdb = True, should_create_mysqldb = True)
            return (code, True)
        except Exception as e:
            return (code, False)

    def update(self):
        if self.init():
            df = self.get(redis = self.redis)
            return concurrent_run(self.create_obj, df.code.tolist(), num = 30)
        return False

    @staticmethod
    def get(redis = None):
        redis = create_redis_obj() if redis is None else redis
        df_byte = redis.get(ct.INDUSTRY_INFO) 
        return pd.DataFrame() if df_byte is None else _pickle.loads(df_byte)

    @staticmethod
    def get_industry_name_dict_from_tongdaxin(fname):
        industry_dict = dict()
        with open(fname, "rb") as f:
            data = f.read()
        info_list = data.decode("gbk").split('######\r\n')
        for info in info_list:
            xlist = info.split('\r\n')
            if xlist[0] == '#TDXNHY':
                zinfo = xlist[1:len(xlist)-1]
        for z in zinfo:
            x = z.split('|')
            industry_dict[x[0]] = x[1]
        return industry_dict

    @staticmethod
    def get_industry_code_dict_from_tongdaxin(fname):
        industry_dict = dict()
        with open(fname, "rb") as f:
            data = f.read()
        str_list = data.decode("utf-8").split('\r\n')
        for x in str_list:
            info_list = x.split('|')
            if len(info_list) == 5:
                industry = info_list[2]
                code = info_list[1]
                if len(industry) == 3: continue #not include B stock and big industry code
                if industry not in industry_dict: industry_dict[industry] = list()
                industry_dict[industry].append(code)
        for key in industry_dict:
            industry_dict[key] = json.dumps(industry_dict[key])
        return industry_dict

    @staticmethod
    def get_csi_industry_data(mdate, fdir = CSI_INDUSRT_DIR):
        # china_security_industry
        file_name = "{}.csv".format(mdate)
        file_path = Path(fdir) / file_name
        dtype_dict = {'code' : str, 'pind_code': int, 'pind_name': str,
                      'sind_code': int, 'sind_name': str, 'tind_code': int, 
                      'tind_name': str, 'find_code': int, 'find_name': str}
        return pd.read_csv(file_path, sep = ',', usecols = dtype_dict.keys(), dtype = dtype_dict)

    @staticmethod
    def get_tdx_industry_code(fname = TONG_DA_XIN_CODE_FILE):
        data = pd.read_csv(TONG_DA_XIN_CODE_FILE, sep = ',', dtype = {'code' : str, 'market': int, 'name': str})
        data = data[['code', 'name']]
        data = data[data.code.str.startswith('880')]
        data = data.reset_index(drop = True)
        return data

    @staticmethod
    def get_industry():
        industry_code_dict = IndustryInfo.get_industry_code_dict_from_tongdaxin(TONG_DA_XIN_CODE_PATH)
        industry_name_dict = IndustryInfo.get_industry_name_dict_from_tongdaxin(TONG_DA_XIN_INDUSTRY_PATH)
        industre_tdx_df = IndustryInfo.get_tdx_industry_code()
        name_list = list()
        for key in industry_code_dict:
            name_list.append(industry_name_dict[key])
        data = {'name':name_list, 'content':list(industry_code_dict.values())}
        df_new = pd.DataFrame.from_dict(data)
        df = pd.merge(df_new, industre_tdx_df, how='left', on=['name'])
        ###exception for difference between tdx and local
        df.at[df.name == '红黄药酒', 'code'] = '880383'
        df.at[df.name == '建筑施工', 'code'] = '880477'
        ################################################
        df = df.drop_duplicates(['name'], keep='first')
        df = df.reset_index(drop = True)
        return df[['code', 'name', 'content']]

if __name__ == '__main__':
    ci = IndustryInfo(ct.DB_INFO)
    df = ci.get_industry()
    #df = df.sort_values(by=['code'])
    #df = df.reset_index(drop = True)
    print(df)
