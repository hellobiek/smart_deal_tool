#coding=utf-8
import os
import re
import sys
import json
import time
import random
import requests
import const as ct
import numpy as np
import pandas as pd
from log import getLogger
from cmysql import CMySQL
from datetime import datetime
logger = getLogger(__name__)

class CLimit:
    def __init__(self, dbinfo):
        self.table = self.get_tbname()
        self.mysql_client = CMySQL(dbinfo)
        self.header = {"Host": "home.flashdata2.jrj.com.cn",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
        if not self.create(): raise Exception("create stock %s failed" % self.table)

    def create(self):
        if self.table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, code varchar(6) not null, price float, pchange float, prange float, concept varchar(50), fcb float, flb float, fdmoney float, first_time varchar(20), last_time varchar(20), open_times int, intensity float, PRIMARY KEY (date, code))' % self.table
            return True if self.mysql_client.create(sql, self.table) else False
        return True

    @staticmethod
    def get_tbname():
        return ct.LIMIT_TABLE  

    def get_useful_columns(self, dtype):
        if dtype == ct.LIMIT_UP or dtype == ct.LIMIT_DOWN:
            return ['代码', '所属概念']
        else:
            return ['代码', '价格', '涨跌幅', '振幅', '封成比', '封流比', '封单金额', '首次涨跌停时间', '最后涨跌停时间', '开板次数', '强度']

    def get_columns(self, dtype):
        #封成比 = 封单金额/日成交额
        #封流比 = 封单手数/流通股本
        if ct.LIMIT_UP == dtype or ct.LIMIT_DOWN == dtype:
            return ['代码', '名称', '涨跌停时间', '价格', '涨跌幅', '成交额', '振幅', '换手率', '五日涨跌幅', '无用', '所属概念（代码）', '所属概念']
        else:
            return ['代码', '名称', '价格', '涨跌幅', '封成比', '封流比', '封单金额', '首次涨跌停时间', '最后涨跌停时间', '开板次数', '振幅', '强度']

    def get_url(self, dtype, date):
        if ct.LIMIT_UP == dtype:
            return ct.URL_PRIFIX + "zt/%s" % date + ct.URL_MID + str(int(round(time.time() * 1000)))
        elif ct.LIMIT_DOWN == dtype:
            return ct.URL_PRIFIX + "dt/%s" % date + ct.URL_MID + str(int(round(time.time() * 1000)))
        elif ct.LIMIT_UP_INTENSITY == dtype:
            return ct.URL_PRIFIX + "ztForce/%s" % date + ct.URL_MID + str(int(round(time.time() * 1000)))
        else:
            return ct.URL_PRIFIX + "dtForce/%s" % date + ct.URL_MID + str(int(round(time.time() * 1000)))

    def get_data_from_url(self, date, dtype, retry = 5):
        response = requests.get(self.get_url(dtype, date), headers=self.header)
        for i in range(retry):
            try:
                content = response.text
                md_check = re.findall(r'"Data":\[\[', content)
                if content and len(md_check) > 0: 
                    return content
            except Exception as e:
                logger.info(e)

    def convert_to_json(self, content):
        if 0 == len(content): return None
        p = re.compile(r'"Data":(.*)};', re.S)
        result = p.findall(content)
        if result:
            try:
                return json.loads(result[0])
            except Exception as e:
                logger.info(e)

    def gen_df(self, dtype, date):
        table = ct.LIMIT_UP if dtype == "UP" else ct.LIMIT_DOWN
        data = self.get_data_from_url(date, table)
        if data is None: return pd.DataFrame()
        limit_up_json_obj = self.convert_to_json(data)
        limit_up_df = pd.DataFrame(limit_up_json_obj, columns=self.get_columns(table))
        limit_up_df = limit_up_df[self.get_useful_columns(table)]

        intense_table = ct.LIMIT_UP_INTENSITY if dtype == "UP" else ct.LIMIT_DOWN_INTENSITY
        data = self.get_data_from_url(date, intense_table)
        if data is None: return pd.DataFrame()
        limit_up_intensity_json_obj = self.convert_to_json(data)
        limit_up_intensity_df = pd.DataFrame(limit_up_intensity_json_obj, columns=self.get_columns(intense_table))
        limit_up_intensity_df = limit_up_intensity_df[self.get_useful_columns(intense_table)]

        df = pd.merge(limit_up_intensity_df, limit_up_df, how='left', on=['代码'])
        df.replace(np.inf, 1000, inplace = True)
        return df

    def get_data(self, date = None):
        date = datetime.now().strftime('%Y-%m-%d') if date is None else date
        return self.mysql_client.get('select * from %s where date=\"%s\"' % (self.table, date))

    def crawl_data(self, date):
        cdate = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d") 
        df_up   = self.gen_df("UP", cdate)
        df_down = self.gen_df("DOWN", cdate)
        df = pd.concat([df_up, df_down])
        if not df.empty:
            df = df.reset_index(drop = True)
            df.columns = ['code', 'price', 'pchange', 'prange', 'fcb', 'flb', 'fdmoney', 'first_time', 'last_time', 'open_times', 'intensity', 'concept']
            df['date'] = date
            if not self.mysql_client.set(df, self.table):
                logger.error("%s get data failed" % date)

if __name__ == '__main__':
    lu = CLimit(ct.DB_INFO)
    lu.crawl_data('2018-08-24')
