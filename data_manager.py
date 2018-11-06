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
from common import get_market_name
from ccalendar import CCalendar
from animation import CAnimation
from index_info import IndexInfo
from industry_info import IndustryInfo
from cstock_info import CStockInfo
from combination import Combination
from datamanager.margin import Margin
from datamanager.hgt import StockConnect
from combination_info import CombinationInfo
from futuquant.common.constant import SubType
import chalted
import traceback
import const as ct
import numpy as np
import pandas as pd
from log import getLogger
from ticks import download, unzip
from datetime import datetime
from subscriber import Subscriber, StockQuoteHandler, TickerHandler
from common import is_trading_time,delta_days,create_redis_obj,add_prifix,add_index_prefix
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
logger = getLogger(__name__)
class DataManager:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo = dbinfo
        self.index_objs = dict()
        self.stock_objs = dict()
        self.combination_objs = dict()
        self.cal_client = CCalendar(dbinfo, redis_host)
        self.comb_info_client = CombinationInfo(dbinfo, redis_host)
        self.stock_info_client = CStockInfo(dbinfo, redis_host)
        self.rindex_stock_data_client = RIndexStock(dbinfo, redis_host) 
        self.index_info_client = IndexInfo(dbinfo, redis_host)
        self.industry_info_client = IndustryInfo(dbinfo, redis_host)
        self.limit_client = CLimit(dbinfo, redis_host)
        self.animation_client = CAnimation(dbinfo, redis_host)
        self.cviewer = CReivew(dbinfo, redis_host)
        self.subscriber = Subscriber()
        self.quote_handler  = StockQuoteHandler()
        self.ticker_handler = TickerHandler()
        self.connect_client = StockConnect(market_from = ct.SH_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL, dbinfo = dbinfo, redis_host = redis_host)
        self.margin_client = Margin(dbinfo = dbinfo, redis_host = redis_host) 

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
        if self.ticker_handler.empty(): return
        datas = self.ticker_handler.getQueue()
        while not datas.empty():
            df = datas.get()
            df = df.set_index('time')
            df.index = pd.to_datetime(df.index)
            for code_str in set(df.code):
                code_id = code_str.split('.')[1]
                self.stock_objs[code_id].run(df.loc[df.code == code_str])

    def init_real_stock_info(self):
        concerned_list = self.get_concerned_list()
        prefix_concerned_list = [add_prifix(code) for code in concerned_list]
        ret = self.subscriber.subscribe(prefix_concerned_list, SubType.TICKER, self.ticker_handler)
        if 0 == ret:
            for code in concerned_list:
                if code not in self.stock_objs:
                    self.stock_objs[code] = CStock(code, self.dbinfo, should_create_influxdb = True, should_create_mysqldb = False)
        return ret

    def init_index_info(self):
        index_list = ct.INDEX_DICT.keys()
        prefix_index_list = [add_index_prefix(code) for code in index_list]
        ret = self.subscriber.subscribe(prefix_index_list, SubType.QUOTE, self.quote_handler)
        if 0 == ret:
            for code in index_list: 
                if code not in self.index_objs:
                    self.index_objs[code] = CIndex(code)
        return ret

    def collect_index_runtime_data(self):
        if self.quote_handler.empty(): return
        datas = self.quote_handler.getQueue()
        obj_pool = Pool(10)
        while not datas.empty():
            df = datas.get()
            df['time'] = df.data_date + ' ' + df.data_time
            df = df.drop(['data_date', 'data_time'], axis = 1)
            df = df.set_index('time')
            df.index = pd.to_datetime(df.index)
            for code_str in set(df.code):
                code_id = code_str.split('.')[1]
                self.index_objs[code_id].run(df.loc[df.code == code_str])

    def run(self, sleep_time):
        while True:
            try:
                if self.cal_client.is_trading_day():
                    if is_trading_time():
                        sleep_time = 1
                        if not self.subscriber.status():
                            self.subscriber.start()
                            if 0 == self.init_index_info() and 0 == self.init_real_stock_info():
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
                            self.subscriber.stop()
            except Exception as e:
                traceback.print_exc()
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
                            if not self.init_yesterday_hk_info():
                                logger.error("init_yesterday_hk_info failed")
                                continue
                            self.set_update_info(10)

                        if finished_step < 11:
                            if not self.init_yesterday_margin():
                                logger.error("init_yesterday_margin failed")
                                continue
                            self.set_update_info(10)
                           
                        if finished_step < 12:
                            if not self.init_today_stock_info():
                                logger.error("init_today_stock_info set failed")
                                continue
                            self.set_update_info(10)

                        #if finished_step < 12:
                        #    if not self.rindex_stock_data_client.set_data():
                        #        logger.error("rindex_stock_data set failed")
                        #        continue

                        #if finished_step < 13:
                        #    if not self.init_base_float_profit():
                        #        logger.error("init base float profit for all stock")
                        #        continue

                        #if finished_step < 11:
                        #    self.cviewer.update()
                        #    self.set_update_info(11)
                        logger.info("updating succeed")
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
                self.combination_objs[str(code_id)] = Combination(code_id, self.dbinfo)

    def init_base_float_profit(self):
        def _set_base_float_profit(code_id):
            _obj = CStock(code_id, should_create_mysqldb = False)
            return (code_id, True) if _obj.set_base_floating_profit() else (code_id, False)

        obj_pool = Pool(500)
        df = self.stock_info_client.get()
        failed_list = df.code.tolist()
        failed_count = 0
        logger.info("enter init_base_float_profit")
        while len(failed_list) > 0:
            is_failed = False
            logger.info("init_base_float_profit:%s" % len(failed_list))
            for result in obj_pool.imap_unordered(_set_base_float_profit, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    is_failed = True
            if is_failed:
                failed_count += 1
                if failed_count > 10: 
                    logger.info("%s base float profit init failed" % failed_list)
                    return False
                time.sleep(10)
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_today_stock_info(self, cdate = None):
        def _set_stock_info(_date, bonus_info, sh_index_info, sz_index_info, code_id):
            _obj = CStock(code_id)
            if get_market_name(code_id) == 'sh':
                return (code_id, True) if _obj.set_k_data(bonus_info, sh_index_info, _date) else (code_id, False) 
            else:
                return (code_id, True) if _obj.set_k_data(bonus_info, sz_index_info, _date) else (code_id, False)

        obj_pool = Pool(500)
        df = self.stock_info_client.get()
        _date = datetime.now().strftime('%Y-%m-%d') if cdate is not None else cdate
        #get shanghai index info
        sh_index_info = CIndex('000001').get_k_data(_date)
        sz_index_info = CIndex('399001').get_k_data(_date)
        #get stock bonus info
        bonus_info  = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
        #failed_list = df.code.tolist()
        failed_list = ['601318', '000001', '002460', '002321', '601288', '601668', '300146', '002153', '600519', '600111', '000400', '601606', '300104', '300188', '002079', '002119', '002129', '002156', '002185', '002218', '002449', '002638', '002654', '002724', '002745', '002815', '002913', '300046', '300053', '300077', '300080', '300102', '300111', '300118', '300223', '300232', '300236', '300241', '300269', '300296', '300301', '300303', '300317', '300323', '300327', '300373', '300389', '300582', '300613', '300623', '300625', '300632', '300671', '300672', '300708', '600151', '600171', '600206', '600360', '600460', '600171', '600206', '600360', '600460', '600537', '600584', '600667', '600703', '601012', '603005', '603501', '603986', '300749']
        cfunc = partial(_set_stock_info, _date, bonus_info, sh_index_info, sz_index_info)
        failed_count = 0
        logger.info("enter init_today_stock_info")
        while len(failed_list) > 0:
            is_failed = False
            logger.info("init_today_stock_info:%s" % len(failed_list))
            for result in obj_pool.imap_unordered(cfunc, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    is_failed = True
            if is_failed:
                failed_count += 1
                if failed_count > 10: 
                    logger.info("%s stock info init failed" % failed_list)
                    return False
                time.sleep(10)
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_today_limit_info(self):
        _date = datetime.now().strftime('%Y-%m-%d')
        return self.limit_client.crawl_data(_date)

    def init_today_industry_info(self):
        def _set_industry_info(code_id):
            return (code_id, CIndex(code_id).set_k_data())
        obj_pool = Pool(50)
        df = self.industry_info_client.get()
        failed_list = df.code.tolist()
        failed_count = 0
        while len(failed_list) > 0:
            is_failed = False
            logger.info("init_today_industry_info:%s" % len(failed_list))
            for result in obj_pool.imap_unordered(_set_industry_info, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    is_failed = True
            if is_failed: 
                failed_count += 1
                if failed_count > 10:
                    logger.info("%s industry init failed" % len(failed_list))
                    return False
                time.sleep(10)
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_yesterday_margin(self):
        self.margin_client.update()

    def init_yesterday_hk_info(self):
        for data in ((ct.SH_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL), (ct.SZ_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL)):
            self.connect_client.set_market(data[0], data[1])
            self.connect_client.update()

    def init_today_index_info(self):
        def _set_index_info(code_id):
            _obj = self.index_objs[code_id] if code_id in self.index_objs else CIndex(code_id)
            return (code_id, _obj.set_k_data())
        obj_pool = Pool(50)
        failed_list = list(ct.TDX_INDEX_DICT.keys())
        failed_count = 0
        while len(failed_list) > 0:
            is_failed = False
            logger.info("init_today_index_info:%s" % len(failed_list))
            for result in obj_pool.imap_unordered(_set_index_info, failed_list):
                if True == result[1]: 
                    failed_list.remove(result[0])
                else:
                    is_failed = True
            if is_failed: 
                failed_count += 1
                if failed_count > 10:
                    logger.info("%s index init failed" % len(failed_list))
                    return False
                time.sleep(10)
        obj_pool.join(timeout = 10)
        obj_pool.kill()
        return True

    def init_all_stock_tick(self):
        black_list = {'000031': ['2018-07-01', '2015-07-01'], '300748':['2018-03-30'], '002142':['2015-07-01'], '600161':['2015-01-05']}
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
            _obj = self.stock_objs[code_id] if code_id in self.stock_objs else CStock(code_id, self.dbinfo)
            for _date in date_only_array:
                if self.cal_client.is_trading_day(_date):
                    if code_id in black_list and _date in black_list[code_id]:
                        continue
                    else:
                        obj_pool.spawn(_obj.set_ticket, _date)
            redis.sadd(ALL_STOCKS, code_id)
            if self.cal_client.is_trading_day() and not self.is_morning_time(): 
                logger.debug("lastest finished index:%s, code:%s, tomorrow continue!" % ((_index + 1), code_id))
                break
        obj_pool.join(timeout = 120)
        obj_pool.kill()

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
    dm = DataManager()
    #cdate = '2018-09-25'
    cdate = None
    dm.init_today_stock_info()
    #dm.init_yesterday_hk_info()
    #dm.init_yesterday_margin()
    #dm.init_today_industry_info()
    #dm.init_today_index_info()
    #dm.init_today_limit_info()
    #dm.init_index_info()
    #print("init index_info success!")
    #dm.collect_index_runtime_data()
    #print("collect index_runtime_data success!")
    #dm.animation_client.collect()
    #print("animation client collect success!")

    #sh_index_obj = CIndex('000001', redis_host='127.0.0.1')
    #sz_index_obj = CIndex('399001', redis_host='127.0.0.1')
    #sh_index_obj.set_k_data(fpath = "/Volumes/data/quant/stock/data/tdx/history/days/%s")
    #sz_index_obj.set_k_data(fpath = "/Volumes/data/quant/stock/data/tdx/history/days/%s")
    #sh_index_info = sh_index_obj.get_k_data()
    #sz_index_info = sz_index_obj.get_k_data()

    #bonus_info = pd.read_csv("/Volumes/data/quant/stock/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    ##for code in ['000001']:
    #for code in ['601318', '000001', '002460', '002321', '601288', '601668', '300146', '002153', '600519', '600111', '000400', '601606', '300104', '300188', '002079', '002119', '002129', '002156', '002185', '002218', '002449', '002638', '002654', '002724', '002745', '002815', '002913', '300046', '300053', '300077', '300080', '300102', '300111', '300118', '300223', '300232', '300236', '300241', '300269', '300296', '300301', '300303', '300317', '300323', '300327', '300373', '300389', '300582', '300613', '300623', '300625', '300632', '300671', '300672', '300708', '600151', '600171', '600206', '600360', '600460', '600171', '600206', '600360', '600460', '600537', '600584', '600667', '600703', '601012', '603005', '603501', '603986', '300749']:
    #    cs = CStock(code, redis_host = '127.0.0.1')
    #    market = get_market_name(code)
    #    index_info = sh_index_info if  market == 'sh' else sz_index_info
    #    logger.info("compute %s" % code)
    #    cs.set_k_data(bonus_info, index_info)
    #    cs.set_base_floating_profit()
