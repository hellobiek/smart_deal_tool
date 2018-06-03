# coding=utf-8
import json
import cmysql
import const as ct
import tushare as ts
import pandas as pd
from log import getLogger
from pandas import DataFrame
from common import trace_func

logger = getLogger(__name__)

# include index and concept in stock
class CombinationInfo:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        self.table = table_name
        self.dbinfo = dbinfo
        self.mysql_client = cmysql.CMySQL(dbinfo)
        if not self.create(): raise Exception("create combination table failed")

    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists %s(name varchar(50), code varchar(10), cType int, content varchar(10000), best varchar(1000))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)

    @trace_func(log = logger)
    def init(self):
        # average index for new data
        df_concept = DataFrame({'name':['average'],'code':['800000'],'cType':[ct.C_AVERAGE],'content':[str(list())]})

        # get concepts from csv file to new_concept_df_concept
        new_concept_df_concept = pd.read_csv(ct.CONCEPT_INPUT)
        new_concept_df_concept['cType'] = ct.C_CONCEPT

        # get old concept form mysql database
        old_df_concept = self.mysql_client.get(ct.SQL % self.table)
        # merge concept dict from file
        if new_concept_df_concept is not None:
            _tmp_df = old_df_concept.append(new_concept_df_concept)
            df_concept = df_concept.append(_tmp_df)

        # get indexes form net to new_index_df_concept
        df_index_dict = {}
        df_index_key_tuple = tuple(ct.INDEX_INFO.keys())
        df_index_code_tuple = tuple(ct.INDEX_INFO.values())
        df_index_type_tuple = tuple([ct.C_INDEX for i in range(len(df_index_key_tuple))])
        df_index_key_list = []
        for i in range(len(df_index_key_tuple)):
            constituents = ts.get_index_constituent(df_index_key_tuple[i])
            if constituents is not None:
                df_index_key_list.append(json.dumps(constituents['code'].tolist(), ensure_ascii = False))
        new_index_df_concept = DataFrame({'name':df_index_key_tuple,'code':df_index_code_tuple,'cType':df_index_type_tuple,'content':df_index_key_list})

        # merge concept dict from file
        if new_index_df_concept is not None:
            df_concept = df_concept.append(new_index_df_concept)
            df_concept.reindex()
            df_concept = df_concept.drop_duplicates('name')

        if df_concept is not None:
            df_concept = df_concept.reset_index(drop = True)
            self.mysql_client.set(df_concept, str(self.table))

    @trace_func(log = logger)
    def get(self, index_type = ct.C_INDEX):
        sql = "select * from %s where cType = %s" % (self.table, index_type)
        return self.mysql_client.get(sql)
