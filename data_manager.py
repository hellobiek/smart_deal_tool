#coding=utf-8
import os
import gc
import time
import json
import _pickle
import datetime
import tushare as ts
from cmysql import CMySQL
from cstock import CStock
from cindex import CIndex
from climit import CLimit 
from gevent.pool import Pool
from functools import partial
from creview import CReivew
from cgreent import CGreenlet
from rstock import RIndexStock 
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
        self.dbinfo = dbinfo
        self.cal_client = CCalendar(dbinfo)
        self.comb_info_client = CombinationInfo(dbinfo)
        self.stock_info_client = CStockInfo(dbinfo)
        self.rindex_stock_data_client = RIndexStock(dbinfo) 
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
        aft_open_hour,aft_open_minute,aft_open_second = (19,00,00)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (23,59,59)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        return aft_open_time < now_time < aft_close_time

    def is_morning_time(self, now_time = None):
        if now_time is None: now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (0,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (6,30,0)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    def collect(self, sleep_time):
        while True:
            try:
                if (not self.cal_client.is_trading_day()) or (self.cal_client.is_trading_day() and self.is_morning_time()):
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
            return ret
        for code in ct.INDEX_DICT:
            if code not in self.index_objs:
                self.index_objs[code] = CIndex(self.dbinfo, code)
        return 0

    def collect_index_runtime_data(self):
        ret, data = self.subscriber.get_quote_data(get_index_list())
        if 0 != ret:
            logger.error("index get subscribe data failed, %s" % data)
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
                    if is_trading_time():
                        sleep_time = 1
                        if not self.subscriber.status():
                            self.subscriber.start()
                            if self.init_index_info() | self.init_real_stock_info() == 0: 
                                self.init_combination_info()
                            else:
                                logger.debug("enter stop dict time")
                                self.subscriber.stop()
                        else:
                            self.collect_stock_runtime_data()
                            self.collect_combination_runtime_data()
                            self.collect_index_runtime_data()
                            self.animation_client.collect()
                    else:
                        sleep_time = 60
                        if self.subscriber.status():
                            logger.debug("enter stop subscriber")
                            self.subscriber.stop()
                            logger.debug("subscriber stop success")
            except Exception as e:
                logger.error(e)
            time.sleep(sleep_time)

    def set_update_info(self, step_length, filename = ct.STEPFILE):
        step_info = dict()
        _date = datetime.now().strftime('%Y-%m-%d')
        step_info[_date] = step_length
        with open(filename, 'w') as f:
            json.dump(step_info, f)

    def get_update_info(self, filename = ct.STEPFILE):
        step_info = dict()
        _date = datetime.now().strftime('%Y-%m-%d')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                step_info = json.load(f)
        return step_info[_date] if _date in step_info else 0

    def update(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day(): 
                    if self.is_collecting_time():
                        finished_step = self.get_update_info()
                        logger.info("enter updating.%s" % finished_step)
                        if finished_step < 1:
                            if not self.cal_client.init(False):
                                logger.error("cal_client init failed")
                                continue
                            self.set_update_info(1)

                        if finished_step < 2:
                            if not self.delisted_info_client.init(False): 
                                logger.error("delisted_info init failed")
                                continue
                            self.set_update_info(2)

                        if finished_step < 3:
                            if not self.stock_info_client.init():
                                logger.error("stock_info init failed")
                                continue
                            self.set_update_info(3)

                        if finished_step < 4:
                            if not self.comb_info_client.init():
                                logger.error("comb_info init failed")
                                continue
                            self.set_update_info(4)

                        if finished_step < 5:
                            if not self.industry_info_client.init():
                                logger.error("industry_info init failed")
                                continue
                            self.set_update_info(5)

                        if finished_step < 6:
                            if not self.download_and_extract():
                                logger.error("download_and_extract failed")
                                continue
                            self.set_update_info(6)

                        if finished_step < 7:
                            if not self.init_today_index_info():
                                logger.error("init_today_index_info failed")
                                continue
                            self.set_update_info(7)

                        if finished_step < 8:
                            if not self.init_today_industry_info():
                                logger.error("init_today_industry_info failed")
                                continue
                            self.set_update_info(8)

                        if finished_step < 9:
                            if not self.init_today_limit_info():
                                logger.error("init_today_limit_info failed")
                                continue
                            self.set_update_info(9)
                           
                        if finished_step < 10:
                            if not self.rindex_stock_data_client.set_data():
                                logger.error("rindex_stock_data set failed")
                                continue
                            self.set_update_info(10)

                        if finished_step < 11:
                            self.cviewer.update()
                            self.set_update_info(11)

                        if finished_step < 12:
                            if not self.init_today_stock_info():
                                logger.error("init_today_stock_info set failed")
                            self.set_update_info(12)
            except Exception as e:
                logger.error(e)
            time.sleep(sleep_time)

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

    def init_today_stock_info(self):
        def _set_stock_info(_date, bonus_info, code_id):
            _obj = CStock(self.dbinfo, code_id)
            if _obj.set_ticket(_date) and _obj.set_k_data(bonus_info): return (code_id, True)
            return (code_id, False)
        obj_pool = Pool(300)
        df = self.stock_info_client.get()
        _date = datetime.now().strftime('%Y-%m-%d')
        bonus_info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
        failed_list = df.code.tolist()
        cfunc = partial(_set_stock_info, _date, bonus_info)
        while len(failed_list) > 0:
            logger.info("init_today_stock_info:%s" % failed_list)
            failed_count = 0
            for result in obj_pool.imap_unordered(cfunc, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    failed_count += 1
            if failed_count > 0 and failed_count == len(failed_list): 
                logger.info("%s stock info init failed" % failed_list)
                return False
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_today_limit_info(self):
        _date = datetime.now().strftime('%Y-%m-%d')
        return self.limit_client.crawl_data(_date)

    def init_today_industry_info(self):
        def _set_industry_info(code_id):
            return (code_id, CIndex(self.dbinfo, code_id).set_k_data())
        obj_pool = Pool(50)
        df = self.industry_info_client.get()
        failed_list = df.code.tolist()
        while len(failed_list) > 0:
            logger.info("init_today_industry_info:%s" % failed_list)
            failed_count = 0
            for result in obj_pool.imap_unordered(_set_industry_info, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    failed_count += 1
            if failed_count > 0 and failed_count == len(failed_list): 
                logger.info("%s industry init failed" % failed_list)
                return False
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_today_index_info(self):
        def _set_index_info(code_id):
            _obj = self.index_objs[code_id] if code_id in self.index_objs else CIndex(self.dbinfo, code_id)
            return (code_id, _obj.set_k_data())
        obj_pool = Pool(50)
        failed_list = ct.TDX_INDEX_DICT.keys()
        while len(failed_list) > 0:
            logger.info("init_today_index_info:%s" % failed_list)
            failed_count = 0
            for result in obj_pool.imap_unordered(_set_index_info, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    failed_count += 1
            if failed_count > 0 and failed_count == len(failed_list): 
                logger.info("%s index init failed" % failed_list)
                return False
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_all_stock_tick(self):
        start_date = '2015-01-01'
        redis = create_redis_obj()
        ALL_STOCKS = 'all_existed_stocks'
        all_stock_set = set(str(stock_id, encoding = "utf8") for stock_id in redis.smembers(ALL_STOCKS)) if redis.exists(ALL_STOCKS) else set()
        _today = datetime.now().strftime('%Y-%m-%d')
        num_days = delta_days(start_date, _today)
        start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, "%Y-%m-%d"))
        data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
        date_only_array = date_only_array[::-1]
        obj_pool = Pool(50)
        df = self.stock_info_client.get()
        for _index, code_id in df.code.iteritems():
            logger.info("all tick index:%s, code:%s" % ((_index + 1), code_id))
            if code_id in all_stock_set: continue
            _obj = self.stock_objs[code_id] if code_id in self.stock_objs else CStock(self.dbinfo, code_id)
            for _date in date_only_array:
                if self.cal_client.is_trading_day(_date):
                    obj_pool.spawn(_obj.set_ticket, _date)
            redis.sadd(ALL_STOCKS, code_id)
            if self.cal_client.is_trading_day() and not self.is_morning_time(): 
                logger.debug("lastest finished index:%s, code:%s, tomorrow continue!" % ((_index + 1), code_id))
                obj_pool.join(timeout = 120)
                obj_pool.kill()
                break
        obj_pool.join()
        obj_pool.kill()

    def init_real_stock_info(self):
        concerned_list = self.get_concerned_list()
        for code_id in concerned_list:
            ret = self.subscriber.subscribe_tick(add_prifix(code_id), CStock)
            if 0 == ret:
                if code_id not in self.stock_objs: self.stock_objs[code_id] = CStock(self.dbinfo, code_id, should_create_influxdb = True, should_create_mysqldb = False)
            else:
                return ret
        return 0

    def download_and_extract(self):
        try:
            download(ct.ZIP_DIR)
            list_files = os.listdir(ct.ZIP_DIR)
            for filename in list_files:
                if not filename.startswith('.'):
                    file_path = os.path.join(ct.ZIP_DIR, filename)
                    if os.path.exists(file_path):
                        unzip(file_path, ct.TIC_DIR)
            return True
        except Exception as e:
            logger.error(e)
            return False
        
if __name__ == '__main__':
    dm = DataManager(ct.DB_INFO) 
    dm.init_today_stock_info()
    #dm.init_today_industry_info()
    #dm.init_today_index_info()
    #dm.init_today_limit_info()
    #dm.init_index_info()
    #print("init index_info success!")
    #dm.collect_index_runtime_data()
    #print("collect index_runtime_data success!")
    #dm.animation_client.collect()
    #print("animation client collect success!")
