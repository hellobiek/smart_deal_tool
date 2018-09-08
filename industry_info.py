# coding=utf-8
import json
import _pickle
import cmysql
import const as ct
import pandas as pd
from cindex import CIndex
from log import getLogger
from pandas import DataFrame
from common import trace_func, create_redis_obj
logger = getLogger(__name__)
class IndustryInfo:
    def __init__(self, dbinfo):
        self.table = ct.INDUSTRY_INFO
        self.redis = create_redis_obj()
        self.mysql_client = cmysql.CMySQL(dbinfo)
        self.mysql_dbs = self.mysql_client.get_all_databases()
        if not self.init(): raise Exception("init combination table failed")

    def init(self):
        new_df = DataFrame()
        new_self_defined_df = self.get_industry()
        new_self_defined_df['best'] = '0'
        new_df = new_df.append(new_self_defined_df)
        new_df = new_df.reset_index(drop = True)
        failed_list = list()
        for _, code_id in new_df['code'].iteritems():
            dbname = CIndex.get_dbname(code_id)
            if dbname not in self.mysql_dbs:
                if not self.mysql_client.create_db(dbname): failed_list.append(code_id)
        if len(failed_list) > 0 :
            logger.error("%s create failed" % failed_list)
            return False
        return self.redis.set(ct.INDUSTRY_INFO, _pickle.dumps(new_df, 2))
        
    @staticmethod
    def get():
        redis = create_redis_obj()
        df_byte = redis.get(ct.INDUSTRY_INFO) 
        return pd.DataFrame() if df_byte is None else _pickle.loads(df_byte)

    def get_industry_name_dict_from_tongdaxin(self, fname):
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

    def get_industry_code_dict_from_tongdaxin(self, fname):
        industry_dict = dict()
        with open(fname, "rb") as f:
            data = f.read()
        str_list = data.decode("utf-8").split('\r\n')
        for x in str_list:
            info_list = x.split('|')
            if len(info_list) == 5:
                industry = info_list[2]
                code = info_list[1]
                if industry == "T00": continue #not include B stock
                if industry not in industry_dict: industry_dict[industry] = list()
                industry_dict[industry].append(code)
        for key in industry_dict:
            industry_dict[key] = json.dumps(industry_dict[key])
        return industry_dict

    def get_tdx_industry_code(self, fname = ct.TONG_DA_XIN_CODE_FILE):
        data = pd.read_csv(ct.TONG_DA_XIN_CODE_FILE, sep = ',', dtype = {'code' : str, 'market': int, 'name': str})
        data = data[['code', 'name']]
        data = data[data.code.str.startswith('880')]
        data = data.reset_index(drop = True)
        return data

    def get_industry(self):
        industry_code_dict = self.get_industry_code_dict_from_tongdaxin(ct.TONG_DA_XIN_CODE_PATH)
        industry_name_dict = self.get_industry_name_dict_from_tongdaxin(ct.TONG_DA_XIN_INDUSTRY_PATH)
        industre_tdx_df = self.get_tdx_industry_code()
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
