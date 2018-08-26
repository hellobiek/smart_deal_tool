#coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
from gevent.pool import Pool
from gevent.event import AsyncResult
import os
import time
import json
import datetime
from cmysql import CMySQL
from cstock import CStock
from cindex import CIndex
from climit import CLimit 
from creview import CReivew
from cdelisted import CDelisted
from ccalendar import CCalendar
from animation import CAnimation
from index_info import IndexInfo
from industry_info import IndustryInfo
from cstock_info import CStockInfo
from combination import Combination
from combination_info import CombinationInfo
import chalted
import traceback
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
from ticks import download, unzip
from pandas import DataFrame
from datetime import datetime
from subscriber import Subscriber
from common import trace_func,is_trading_time,delta_days,create_redis_obj,add_prifix,get_index_list,add_index_prefix
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
logger = getLogger(__name__)
class DataManager:
    def __init__(self, dbinfo):
        self.combination_objs = dict()
        self.stock_objs = dict()
        self.index_objs = dict()
        self.evt = AsyncResult()
        self.dbinfo = dbinfo
        self.cal_client = CCalendar(dbinfo)
        self.comb_info_client = CombinationInfo(dbinfo)
        self.stock_info_client = CStockInfo(dbinfo)
        self.index_info_client = IndexInfo(dbinfo)
        self.industry_info_client = IndustryInfo(dbinfo)
        self.delisted_info_client = CDelisted(dbinfo)
        self.limit_client = CLimit(dbinfo)
        self.animation_client = CAnimation(dbinfo)
        self.subscriber = Subscriber()
        self.cviewer = CReivew(dbinfo)

    def is_collecting_time(self, now_time = None):
        if now_time is None: now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        aft_open_hour,aft_open_minute,aft_open_second = (19,00,0)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (23,59,59)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        return aft_open_time < now_time < aft_close_time

    def collect(self, sleep_time):
        while True:
            try:
                self.init_all_stock_tick()
            except Exception as e:
                logger.error(e)
            time.sleep(sleep_time)

    def collect_combination_runtime_data(self):
        obj_pool = Pool(10)
        for code_id in self.combination_objs:
            try:
                if obj_pool.full(): obj_pool.join()
                obj_pool.spawn(self.combination_objs[code_id].run)
            except Exception as e:
                logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def collect_stock_runtime_data(self):
        obj_pool = Pool(100)
        for code_id in self.stock_objs:
            try:
                if obj_pool.full(): obj_pool.join()
                ret, df = self.subscriber.get_tick_data(add_prifix(code_id))
                if 0 == ret:
                    df = df.set_index('time')
                    df.index = pd.to_datetime(df.index)
                    obj_pool.spawn(self.stock_objs[code_id].run, df)
            except Exception as e:
                logger.info(e)
        obj_pool.join()
        obj_pool.kill()
    
    def init_index_info(self):
        ret, data = self.subscriber.subscribe_quote(get_index_list())
        if 0 != ret: 
            logger.error("index subscribe failed")
            return
        for code in ct.INDEX_DICT:
            if code not in self.index_objs:
                self.index_objs[code] = CIndex(self.dbinfo, code)

    def collect_index_runtime_data(self):
        ret, data = self.subscriber.get_quote_data(get_index_list())
        if 0 != ret:
            logger.error("index get subscribe data failed")
            return
        obj_pool = Pool(10)
        for code in self.index_objs:
            code_str = add_index_prefix(code)
            df = data[data.code == code_str]
            df = df.reset_index(drop = True)
            df['time'] = df.data_date + ' ' + df.data_time
            df = df.drop(['data_date', 'data_time'], axis = 1)
            df = df.set_index('time')
            df.index = pd.to_datetime(df.index)
            try:
                if obj_pool.full(): obj_pool.join()
                if 0 == ret: obj_pool.spawn(self.index_objs[code].run, df)
            except Exception as e:
                logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def run(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day():
                    if is_trading_time() and not self.subscriber.status():
                        self.subscriber.start()
                        self.init_index_info()
                        self.init_real_stock_info()
                        self.init_combination_info()
                    elif is_trading_time() and self.subscriber.status():
                        self.collect_stock_runtime_data()
                        self.collect_combination_runtime_data()
                        self.collect_index_runtime_data()
                        self.animation_client.collect()
                    elif not is_trading_time() and self.subscriber.status():
                        self.subscriber.stop()
            except Exception as e:
                logger.error(e)
                traceback.print_exc()
            time.sleep(sleep_time)

    def update(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day(): 
                    if self.is_collecting_time():
                        self.init()
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(e)
                #traceback.print_exc()

    def init(self, status = False):
        #self.halted_info_client.init(status)
        self.cal_client.init(status)
        self.delisted_info_client.init(status)
        self.stock_info_client.init()
        self.comb_info_client.init()
        self.industry_info_client.init()
        self.download_and_extract()
        self.init_today_stock_tick()
        self.init_today_index_info()
        self.init_today_industry_info()
        self.init_today_limit_info()
        self.cviewer.update()

    def get_concerned_list(self):
        combination_info = self.comb_info_client.get()
        if combination_info is None: return list()
        combination_info = combination_info.reset_index(drop = True)
        res_list = list()
        for index, _ in combination_info['code'].iteritems():
            objliststr = combination_info.loc[index]['content']
            objlist = objliststr.split(',')
            res_list.extend(objlist)
        return list(set(res_list))

    def init_combination_info(self):
        trading_info = self.comb_info_client.get()
        for _, code_id in trading_info['code'].iteritems():
            if str(code_id) not in self.combination_objs:
                self.combination_objs[str(code_id)] = Combination(self.dbinfo, code_id)

    def init_today_stock_tick(self):
        _date = datetime.now().strftime('%Y-%m-%d')
        obj_pool = Pool(50)
        df = self.stock_info_client.get()
        if self.cal_client.is_trading_day(_date):
            for _, code_id in df.code.iteritems():
                _obj = self.stock_objs[code_id] if code_id in self.stock_objs else CStock(self.dbinfo, code_id)
                try:
                    if obj_pool.full(): obj_pool.join()
                    obj_pool.spawn(_obj.set_ticket, _date)
                    obj_pool.spawn(_obj.set_k_data)
                except Exception as e:
                    logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def init_today_limit_info(self):
        _date = datetime.now().strftime('%Y-%m-%d')
        self.limit_client.crawl_data(_date)

    def init_today_industry_info(self):
        obj_pool = Pool(50)
        df = self.industry_info_client.get()
        for _, code_id in df.code.iteritems():
            _obj = CIndex(self.dbinfo, code_id)
            try:
                if obj_pool.full(): obj_pool.join()
                obj_pool.spawn(_obj.set_k_data)
            except Exception as e:
                logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def init_today_index_info(self):
        obj_pool = Pool(50)
        for code_id in ct.TDX_INDEX_DICT:
            _obj = self.index_objs[code_id] if code_id in self.index_objs else CIndex(self.dbinfo, code_id)
            try:
                if obj_pool.full(): obj_pool.join()
                obj_pool.spawn(_obj.set_k_data)
            except Exception as e:
                logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def init_all_stock_tick(self):
        start_date = '2015-01-01'
        _today = datetime.now().strftime('%Y-%m-%d')
        num_days = delta_days(start_date, _today)
        start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        date_only_array = date_only_array[::-1]
        obj_pool = Pool(6)
        df = self.stock_info_client.get()
        for _, code_id in df.code.iteritems():
            _obj = self.stock_objs[code_id] if code_id in self.stock_objs else CStock(self.dbinfo, code_id)
            for _date in date_only_array:
                if self.cal_client.is_trading_day(_date):
                    try:
                        if obj_pool.full(): obj_pool.join()
                        obj_pool.spawn(_obj.set_ticket, _date)
                    except Exception as e:
                        logger.info(e)
        obj_pool.join()
        obj_pool.kill()

    def init_real_stock_info(self):
        concerned_list = self.get_concerned_list()
        for code_id in concerned_list:
            ret = self.subscriber.subscribe_tick(add_prifix(code_id), CStock)
            if 0 == ret:
                if code_id not in self.stock_objs: self.stock_objs[code_id] = CStock(self.dbinfo, code_id)

    def download_and_extract(self):
        while True:
            try:
                download(ct.ZIP_DIR)
                list_files = os.listdir(ct.ZIP_DIR)
                for filename in list_files:
                    if not filename.startswith('.'):
                        file_path = os.path.join(ct.ZIP_DIR, filename)
                        if os.path.exists(file_path):unzip(file_path, ct.TIC_DIR)
            except Exception as e:
                logger.error(e)
        
if __name__ == '__main__':
    dm = DataManager(ct.DB_INFO)
    dm.init_today_index_info()
    print("success")
    #dm.init_today_industry_info()
    #dm.init_today_limit_info()
    #dm.init_index_info()
    #print("init index_info success!")
    #dm.collect_index_runtime_data()
    #print("collect index_runtime_data success!")
    #dm.animation_client.collect()
    #print("animation client collect success!")
