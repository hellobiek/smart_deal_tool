#coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
from gevent.pool import Pool
from gevent.event import AsyncResult
import sys
import pdb
import time
import json
import datetime
import ccalendar
import cstock
import chalted
import traceback
import cstock_info
import cdelisted
import combination
import combination_info
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame
from log import getLogger
from datetime import datetime
from common import trace_func,is_trading_time,delta_days,create_redis_obj
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
logger = getLogger(__name__)

class DataManager:
    def __init__(self, dbinfo, stock_info_table, combination_info_table, calendar_table, delisted_table, halted_table):
        self.objs = dict()
        self.evt = AsyncResult()
        self.dbinfo = dbinfo
        self.cal_client = ccalendar.CCalendar(dbinfo, calendar_table)
        self.comb_info_client = combination_info.CombinationInfo(dbinfo, combination_info_table)
        self.stock_info_client = cstock_info.CStockInfo(dbinfo, stock_info_table)
        self.delisted_info_client = cdelisted.CDelisted(dbinfo, delisted_table)
        self.halted_info_client = chalted.CHalted(dbinfo, halted_table)

    def is_collecting_time(self, now_time = None):
        if now_time is None:now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (18,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    def collect(self, sleep_time):
        time.sleep(30)
        while True:
            try:
                if self.cal_client.is_trading_day():
                    #if self.is_collecting_time():
                    self.init_all_stock_tick()
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(e)
                traceback.print_exc()

    def run(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day():
                    if is_trading_time():
                        self.init_combination_info()
                        self.init_real_stock_info()
                        self.collect_realtime_info()
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(e)
                traceback.print_exc()

    def update(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day(): 
                    if self.is_collecting_time():
                        self.init()
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(e)
                traceback.print_exc()

    def init(self, status = False):
        self.cal_client.init(status)
        self.comb_info_client.init()
        self.stock_info_client.init()
        self.delisted_info_client.init(status)
        self.halted_info_client.init(status)

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
            if str(code_id) not in self.objs: 
                self.objs[str(code_id)] = combination.Combination(self.dbinfo, code_id)

    def init_all_stock_tick(self):
        df = self.stock_info_client.get()
        start_date = ct.START_DATE
        _today = datetime.now().strftime('%Y-%m-%d')
        num_days = delta_days(start_date, _today)
        start_date_dmy_format = time.strftime("%d/%m/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        logger.info("init_all_stock_tick")
        for _date in date_only_array:
            if self.cal_client.is_trading_day(_date):
                for _, code_id in df.code.iteritems():
                    if code_id not in self.objs:
                        self.objs[code_id] = cstock.CStock(self.dbinfo, code_id)
                    try:
                        self.objs[code_id].set_ticket(_date)
                        time.sleep(3)
                    except Exception as e:
                        logger.info(e)

    def init_real_stock_info(self):
        concerned_list = self.get_concerned_list()
        for code_id in concerned_list:
            if code_id not in self.objs:
                self.objs[code_id] = cstock.CStock(self.dbinfo, code_id)

    def get_all_info_from_remote(self, stock_list):
        all_info = None
        start_index = 0
        stock_nums = len(stock_list)
        while start_index < stock_nums - 1:
            end_index = stock_nums - 1 if start_index + 20 > stock_nums else start_index + 20
            stock_codes = stock_list[start_index:end_index]
            _info = ts.get_realtime_quotes(stock_codes)
            all_info = _info if all_info is None else all_info.append(_info)
            start_index = end_index
        if all_info is not None:
            convert_list = ['b1_v', 'b2_v', 'b3_v', 'b4_v', 'b5_v', 'a1_v', 'a2_v', 'a3_v', 'a4_v', 'a5_v']
            for conver_str in convert_list:
                all_info[conver_str] = pd.to_numeric(_info[conver_str], errors='coerce')
            all_info['limit_up_time'] = 0
            all_info['limit_down_time'] = 0
            all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
            now_time = datetime.now().strftime('%H-%M-%S')
            all_info[all_info["p_change"]>9.9]['limit_up_time'] = now_time
            all_info[all_info["p_change"]<-9.9]['limit_down_time'] = now_time
        #too often visit will cause net error 
        time.sleep(1)
        #logger.info("get_all_info_from_remote:%s" % all_info)
        self.evt.set(all_info)

    def collect_realtime_info(self):
        obj_pool = Pool(200)
        stock_list = self.get_concerned_list()
        self.get_all_info_from_remote(stock_list)
        obj_list = self.objs.keys()
        for key in obj_list:
            if obj_pool.full(): obj_pool.join()
            obj_pool.spawn(self.objs[key].run, self.evt)
        obj_pool.join()
        obj_pool.kill()

if __name__ == '__main__':
    dm = DataManager(ct.DB_INFO, ct.STOCK_INFO_TABLE, ct.COMBINATION_INFO_TABLE, ct.CALENDAR_TABLE, ct.DELISTED_INFO_TABLE, ct.HALTED_TABLE)
    dm.update(5)
