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
from cmysql import CMySQL
from base.clog import getLogger
from ccalendar import CCalendar
from common import create_redis_obj, get_day_nday_ago, get_dates_array
from datetime import datetime
class CLimit:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.table = self.get_table_name()
        self.logger = getLogger(__name__)
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client = CMySQL(dbinfo, iredis = self.redis)
        self.header = {"Host": "home.flashdata2.jrj.com.cn",
                       "Referer": "http://stock.jrj.com.cn/tzzs/zdtwdj/zdforce.shtml",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
        if not self.create(): raise Exception("create stock %s failed" % self.table)

    def create(self):
        if self.table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 code varchar(6) not null,\
                                                 price float,\
                                                 pchange float,\
                                                 prange float,\
                                                 concept varchar(50),\
                                                 fcb float,\
                                                 flb float,\
                                                 fdmoney float,\
                                                 first_time varchar(20),\
                                                 last_time varchar(20),\
                                                 open_times varchar(20),\
                                                 intensity float,\
                                                 PRIMARY KEY (date, code))' % self.table
            return True if self.mysql_client.create(sql, self.table) else False
        return True

    @staticmethod
    def get_table_name():
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
            return ct.LIMIT_URL_PRIFIX + "zt/%s" % date + ct.LIMIT_URL_MID + str(int(round(time.time() * 1000)))
        elif ct.LIMIT_DOWN == dtype:
            return ct.LIMIT_URL_PRIFIX + "dt/%s" % date + ct.LIMIT_URL_MID + str(int(round(time.time() * 1000)))
        elif ct.LIMIT_UP_INTENSITY == dtype:
            return ct.LIMIT_URL_PRIFIX + "ztForce/%s" % date + ct.LIMIT_URL_MID + str(int(round(time.time() * 1000)))
        else:
            return ct.LIMIT_URL_PRIFIX + "dtForce/%s" % date + ct.LIMIT_URL_MID + str(int(round(time.time() * 1000)))

    def get_data_from_url(self, date, dtype, retry_times = 5):
        for i in range(retry_times):
            try:
                response = requests.get(self.get_url(dtype, date), headers=self.header)
                if response.status_code == 200:
                    content = response.text
                    return content
            except Exception as e:
                self.logger.error(e)
            time.sleep(i * 2)
        return 

    def convert_to_json(self, content):
        if 0 == len(content): return None
        p = re.compile(r'"Data":(.*)};', re.S)
        result = p.findall(content)
        if result:
            try:
                return json.loads(result[0])
            except Exception as e:
                self.logger.info(e)

    def gen_df(self, dtype, cdate):
        cdate = datetime.strptime(cdate, "%Y-%m-%d").strftime("%Y%m%d")
        table = ct.LIMIT_UP if dtype == "UP" else ct.LIMIT_DOWN
        data = self.get_data_from_url(cdate, table)
        if data is None: return
        limit_up_json_obj = self.convert_to_json(data)
        limit_up_df = pd.DataFrame(limit_up_json_obj, columns=self.get_columns(table))
        limit_up_df = limit_up_df[self.get_useful_columns(table)]

        intense_table = ct.LIMIT_UP_INTENSITY if dtype == "UP" else ct.LIMIT_DOWN_INTENSITY
        data = self.get_data_from_url(cdate, intense_table)
        if data is None: return
        limit_up_intensity_json_obj = self.convert_to_json(data)
        limit_up_intensity_df = pd.DataFrame(limit_up_intensity_json_obj, columns=self.get_columns(intense_table))
        limit_up_intensity_df = limit_up_intensity_df[self.get_useful_columns(intense_table)]

        df = pd.merge(limit_up_intensity_df, limit_up_df, how='left', on=['代码'])
        df.replace(np.inf, 1000, inplace = True)
        return df

    def get_data(self, cdate = None):
        if cdate is None: cdate = datetime.now().strftime('%Y-%m-%d') 
        return self.mysql_client.get('select * from %s where date=\"%s\"' % (self.table, cdate))

    def crawl_data(self, cdate):
        if self.is_date_exists(self.table, cdate):
            self.logger.debug("existed table:%s, date:%s" % (self.table, cdate))
            return True

        df_up = self.gen_df("UP", cdate)
        df_down = self.gen_df("DOWN", cdate)

        if df_up is None or df_down is None: return False
        df = pd.concat([df_up, df_down], sort=True)
        if df.empty: return False
        df = df.reset_index(drop = True)
        df.columns = ['code', 'price', 'fdmoney', 'fcb', 'flb', 'open_times', 'intensity', 'concept', 'prange', 'last_time', 'pchange', 'first_time']
        df['date'] = cdate
        if self.mysql_client.set(df, self.table):
            return self.redis.sadd(self.table, cdate)
        return False

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(tdate.decode() for tdate in self.redis.smembers(table_name))
        return False

    def update(self, end_date = None, num = 30):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        date_array = get_dates_array(start_date, end_date)
        succeed = True
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                #if mdate == end_date: continue
                if not self.crawl_data(mdate):
                    self.logger.error("%s set failed" % mdate)
                    succeed = False
        return succeed

if __name__ == '__main__':
    cl = CLimit()
    #cl.mysql_client.delete(cl.table)
    cl.update(end_date = '2019-03-28')
