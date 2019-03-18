#coding=utf-8
import os
import time
import json
import gevent
import schedule
import datetime
import traceback
import const as ct
import numpy as np
import pandas as pd
from log import getLogger
from cstock import CStock
from cindex import CIndex, TdxFgIndex
from climit import CLimit 
from functools import partial
from datetime import datetime
from rstock import RIndexStock
from ccalendar import CCalendar
from animation import CAnimation
from index_info import IndexInfo
from ticks import download, unzip
from cstock_info import CStockInfo
from combination import Combination
from industry_info import IndustryInfo
from datamanager.margin  import Margin
from datamanager.emotion import Emotion
from datamanager.bull_stock_ratio import BullStockRatio
from datamanager.hgt import StockConnect
from datamanager.sexchange import StockExchange
from rindustry import RIndexIndustryInfo
from combination_info import CombinationInfo
from futu.common.constant import SubType
from crawler.dspider.hkex import HkexCrawler
from crawler.dspider.run import start_spider
from subscriber import Subscriber, StockQuoteHandler, TickerHandler
from common import is_trading_time, delta_days, create_redis_obj, add_prifix, add_index_prefix, kill_process, concurrent_run, get_day_nday_ago, get_dates_array, process_concurrent_run
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
class DataManager:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo = dbinfo
        self.logger = getLogger(__name__)
        self.index_objs = dict()
        self.stock_objs = dict()
        self.combination_objs = dict()
        self.cal_client = CCalendar(dbinfo, redis_host)
        self.index_info_client = IndexInfo()
        self.comb_info_client = CombinationInfo(dbinfo, redis_host)
        self.stock_info_client = CStockInfo(dbinfo, redis_host)
        self.rindex_stock_data_client = RIndexStock(dbinfo, redis_host) 
        self.industry_info_client = IndustryInfo(dbinfo, redis_host)
        self.rindustry_info_client = RIndexIndustryInfo(dbinfo, redis_host)
        self.limit_client = CLimit(dbinfo, redis_host)
        self.animation_client = CAnimation(dbinfo, redis_host)
        self.subscriber = Subscriber()
        self.quote_handler  = StockQuoteHandler()
        self.ticker_handler = TickerHandler()
        self.connect_client = StockConnect(market_from = ct.SH_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL, dbinfo = dbinfo, redis_host = redis_host)
        self.margin_client = Margin(dbinfo = dbinfo, redis_host = redis_host) 
        self.emotion_client = Emotion(dbinfo = dbinfo, redis_host = redis_host)
        self.sh_exchange_client = StockExchange(ct.SH_MARKET_SYMBOL)
        self.sz_exchange_client = StockExchange(ct.SZ_MARKET_SYMBOL)

    def is_collecting_time(self, now_time = datetime.now()):
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        aft_open_hour,aft_open_minute,aft_open_second = (18,30,00)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (23,59,59)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        return aft_open_time < now_time < aft_close_time

    def is_morning_time(self, now_time = datetime.now()):
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (0,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (6,30,0)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    def collect_combination_runtime_data(self):
        def _combination_run(code_id):
            self.combination_objs[code_id].run()
            return (code_id, True)
        todo_iplist = list(self.combination_objs.keys())
        return concurrent_run(_combination_run, todo_iplist, num = 10)

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
        concerned_list = self.comb_info_client.get_concerned_list()
        prefix_concerned_list = [add_prifix(code) for code in concerned_list]
        ret = self.subscriber.subscribe(prefix_concerned_list, SubType.TICKER, self.ticker_handler)
        if 0 == ret:
            for code in concerned_list:
                if code not in self.stock_objs:
                    self.stock_objs[code] = CStock(code, self.dbinfo, should_create_influxdb = True, should_create_mysqldb = True)
        return ret

    def init_index_info(self):
        index_list = ct.INDEX_DICT.keys()
        prefix_index_list = [add_index_prefix(code) for code in index_list]
        ret = self.subscriber.subscribe(prefix_index_list, SubType.QUOTE, self.quote_handler)
        if 0 != ret:
            self.logger.error("subscribe for index list failed")
            return ret
        for code in index_list: 
            if code not in self.index_objs:
                self.index_objs[code] = CIndex(code, should_create_influxdb = True, should_create_mysqldb = True)
        return 0

    def collect_index_runtime_data(self):
        if self.quote_handler.empty(): return
        datas = self.quote_handler.getQueue()
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
                        t_sleep_time = 1
                        if not self.subscriber.status():
                            self.subscriber.start()
                            if 0 == self.init_index_info() and 0 == self.init_real_stock_info():
                                self.init_combination_info()
                            else:
                                self.logger.debug("enter stop subscriber")
                                self.subscriber.stop()
                        else:
                            self.collect_stock_runtime_data()
                            self.collect_combination_runtime_data()
                            self.collect_index_runtime_data()
                            self.animation_client.collect()
                    else:
                        t_sleep_time = sleep_time
                        if self.subscriber.status():
                            self.subscriber.stop()
                else:
                    t_sleep_time = sleep_time
            except Exception as e:
                #traceback.print_exc()
                self.logger.error(e)
            gevent.sleep(t_sleep_time)

    def set_update_info(self, step_length, exec_date, cdate = None, filename = ct.STEPFILE):
        step_info = dict()
        if cdate is None: cdate = 'none'
        step_info[cdate] = dict()
        step_info[cdate]['step'] = step_length
        step_info[cdate]['date'] = exec_date
        with open(filename, 'w') as f:
            json.dump(step_info, f)
        self.logger.info("finish step :%s" %  step_length)

    def get_update_info(self, cdate = None, exec_date = None, filename = ct.STEPFILE):
        if cdate is None: cdate = 'none'
        if not os.path.exists(filename): return (0, exec_date)
        with open(filename, 'r') as f: step_info = json.load(f)
        if cdate not in step_info: return (0, exec_date)
        return (step_info[cdate]['step'], step_info[cdate]['date'])

    def bootstrap(self, cdate = None, exec_date = datetime.now().strftime('%Y-%m-%d')):
        finished_step, exec_date = self.get_update_info(cdate, exec_date)
        self.logger.info("enter updating.%s" % finished_step)
        if finished_step < 1:
            if not self.cal_client.init():
                self.logger.error("cal client init failed")
                return False
            self.set_update_info(1, exec_date, cdate)

        if finished_step < 2:
            if not self.index_info_client.update():
                self.logger.error("index info init failed")
                return False
            self.set_update_info(2, exec_date, cdate)

        if finished_step < 3:
            if not self.stock_info_client.update():
                self.logger.error("stock info init failed")
                return False
            self.set_update_info(3, exec_date, cdate)

        if finished_step < 4:
            if not self.comb_info_client.update():
                self.logger.error("comb info init failed")
                return False
            self.set_update_info(4, exec_date, cdate)

        if finished_step < 5:
            if not self.industry_info_client.update():
                self.logger.error("industry info init failed")
                return False
            self.set_update_info(5, exec_date, cdate)

        if finished_step < 6:
            if not self.download_and_extract(exec_date):
                self.logger.error("download and extract failed")
                return False
            self.set_update_info(6, exec_date, cdate)

        if finished_step < 7:
            if not self.init_tdx_index_info(cdate):
                self.logger.error("init tdx index info failed")
                return False
            self.set_update_info(7, exec_date, cdate)

        if finished_step < 8:
            if not self.sh_exchange_client.update(exec_date, num = 30):
                self.logger.error("sh exchange update failed")
                return False
            self.set_update_info(8, exec_date, cdate)

        if finished_step < 9:
            if not self.sz_exchange_client.update(exec_date, num = 30):
                self.logger.error("sz exchange update failed")
                return False
            self.set_update_info(9, exec_date, cdate)

        if finished_step < 10:
            if not self.init_index_components_info(exec_date):
                self.logger.error("init index components info failed")
                return False
            self.set_update_info(10, exec_date, cdate)

        if finished_step < 11:
            if not self.init_industry_info(cdate):
                self.logger.error("init industry info failed")
                return False
            self.set_update_info(11, exec_date, cdate)

        if finished_step < 12:
            if not self.rindustry_info_client.update(exec_date):
                self.logger.error("init %s rindustry info failed" % exec_date)
                return False
            self.set_update_info(12, exec_date, cdate)

        if finished_step < 13:
            if not self.limit_client.update(exec_date):
                self.logger.error("init limit info failed")
                return False
            self.set_update_info(13, exec_date, cdate)

        if finished_step < 14:
            if not self.init_yesterday_hk_info(exec_date):
                self.logger.error("init yesterday hk info failed")
                return False
            self.set_update_info(14, exec_date, cdate)

        if finished_step < 15:
            if not self.margin_client.update(exec_date):
                self.logger.error("init yesterday margin failed")
                return False
            self.set_update_info(15, exec_date, cdate)

        if finished_step < 16:
            if not self.init_stock_info(cdate):
                self.logger.error("init stock info set failed")
                return False
            self.set_update_info(16, exec_date, cdate)

        if finished_step < 17:
            if not self.init_base_float_profit():
                self.logger.error("init base float profit for all stock")
                return False
            self.set_update_info(17, exec_date, cdate)

        if finished_step < 18:
            if not self.rindex_stock_data_client.update(exec_date, num = 30):
                self.logger.error("rstock data set failed")
                return False
            self.set_update_info(18, exec_date, cdate)

        if finished_step < 19:
            if not self.set_bull_stock_ratio(exec_date, num = 30):
                self.logger.error("bull ratio set failed")
                return False
            self.set_update_info(19, exec_date, cdate)

        self.logger.info("updating succeed")
        return True

    def clear_network_env(self):
        kill_process("google-chrome")
        kill_process("renderer")
        kill_process("Xvfb")
        kill_process("zygote")
        kill_process("defunct")
        kill_process("show-component-extension-options")

    def update(self, sleep_time):
        succeed = False
        while True:
            self.logger.debug("enter daily update process. %s" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            try:
                if self.cal_client.is_trading_day(): 
                    self.logger.info("is trading day. %s" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    if self.is_collecting_time() and not succeed:
                        self.logger.info("is collecting time. %s" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        self.clear_network_env()
                        mdate = datetime.now().strftime('%Y-%m-%d')
                        succeed = self.bootstrap(cdate = mdate, exec_date = mdate)
                    else:
                        if not self.is_collecting_time(): succeed = False
                        gevent.sleep(sleep_time)
            except Exception as e:
                time.sleep(1)
                self.logger.error(e)

    def init_combination_info(self):
        trading_info = self.comb_info_client.get()
        for _, code_id in trading_info['code'].iteritems():
            if str(code_id) not in self.combination_objs:
                self.combination_objs[str(code_id)] = Combination(code_id, self.dbinfo)

    def init_base_float_profit(self):
        def _set_base_float_profit(code_id):
            if CStock(code_id).set_base_floating_profit():
                self.logger.info("%s set base float profit success" % code_id)
                return (code_id, True)
            else:
                self.logger.error("%s set base float profit failed" % code_id)
                return (code_id, False)
        failed_list = self.stock_info_client.get().code.tolist()
        self.logger.info("%s stocks to be set" % len(failed_list))
        return process_concurrent_run(_set_base_float_profit, failed_list, num = 500)

    def compute_base_float_profit(self):
        def _compute_base_float_profit(code_id):
            if CStock(code_id).compute_floating_profit():
                self.logger.info("%s set base float profit success" % code_id)
                return (code_id, True)
            else:
                self.logger.error("%s set base float profit failed" % code_id)
                return (code_id, False)
        failed_list = self.stock_info_client.get().code.tolist()
        self.logger.info("%s stocks to be set" % len(failed_list))
        return process_concurrent_run(_compute_base_float_profit, failed_list, num = 500)

    def init_stock_info(self, cdate = None):
        def _set_stock_info(_date, bonus_info, index_info, code_id):
            try:
                if CStock(code_id).set_k_data(bonus_info, index_info, _date):
                    self.logger.info("%s set k data success" % code_id)
                    return (code_id, True)
                else:
                    self.logger.error("%s set k data failed" % code_id)
                    return (code_id, False)
            except Exception as e:
                self.logger.error("%s set k data exception:%s" % (code_id, e))
                return (code_id, False)

        #get stock bonus info
        bonus_info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',',
                      dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})

        index_info = CIndex('000001').get_k_data()
        if index_info is None or index_info.empty: return False
        #df = self.stock_info_client.get()
        #failed_list = df.code.tolist()
        failed_list = ['002535','300062','002783','002560','002471','000532','300584','300215','600018','600438','600160','000635','300014','002912','300595','002540',\
                       '603177','600630','300607','600874','000678','600875','300005','002755','600020','002949','603803','300199','300385','300383','000738','600798',\
                       '603040','000544','000030','300699','300398','603599','002284','300311','000973','002377','002642','002191','300371','603596','300537','002638',\
                       '000637','300185','603896','000970','300129','600699','300606','600435','300235','600199','002141','002530','603660','300579','600982','002101',\
                       '300433','000020','002048','300402','600537','000875','002233','300299','300711','603067','600603','002456','000531','600513','000413','300448',\
                       '002690','002224','300041','002205','603828','600321','603188','002570','300465','002559','300387','002664','300045','603486','300313','002392',\
                       '600666','000728','002809','300540','002290','300075','000701','002646','002418','002292','000939','002232','000430','600639','300479','002681',\
                       '002738','603042','300500','002588','002129','603515','600051','002928','002164','000905','300390','002531','002363','603869','002281','300565',\
                       '002846','002036','002694','300121','300528','002348','603897','002659','300668','603800','600213','603607','300429','002010','000576','002520',\
                       '002422','603533','300357','300400','603895','002132','300610','601886','300306','300348','603199','600744','600884','000404','603036','600292',\
                       '600455','600983','600538','300708','300608','000596','603977','300420','002148','002008','603009','600187','002184','600651','002651','002803',\
                       '603079','603286','002109','000021','603338','603678','600771','603855','300359','300709','601038','600279','300029','300497','603085','300707',\
                       '600841','300526','300198','000610','603838','300089','002654','000822','601678','002675','300441','600780','000155','600682','603300','002145',\
                       '000564','300003','300346','600496','300243','002554','000078','600006','300460','300006','000928','000676','002073','600712','002929','300636',\
                       '002650','300482','002938','600500','603991','600368','300301','000848','002223','300430','300314','002825','600387','002413','002493','002574',\
                       '603683','600179','600794','002561','300310','600619','000829','000908','300622','002686','600391','002815','600491','603227','300384','002230',\
                       '600609','300312','300650','000004','600641','000561','002868','603117','002352','000502','300172','600309','300223','000661','300135','002739',\
                       '603583','002731','603758','002701','300298','002903','000570','600125','600746','002544','300355','002688','300266','002201','300222','000151',\
                       '002769','002503','600203','002649','300745','002490','300426','600735','002641','600960','002315','002211','601991','000616','002144','300623',\
                       '002056','300586','000565','002566','603113','002046','600458','300166','600358','601002','600069','002833','600880','002173','002509','600108',\
                       '300297','000931','000623','002872','603866','600189','000019','002703','000063','300240','001696','300078','300214','000686','002193','603616',\
                       '600122','002246','603668','002910','600206','600566','300518','000408','300457','000852','000980','300740','600528','603339','002637','300255',\
                       '300253','600146','002774','600367','300163','000509','600977','600770','002717','002553','603039','600785','002386','300414','600536','002748',\
                       '600156','002898','600677','000719','300169','600812','000777','600748','603767','600594','000903','300410','002338','002347','002719','002666',\
                       '600257','002001','002489','002291','002793','603088','002060','300454','000615','300760','000997','002296','300103','300123','600814','000407',\
                       '000860','600141','600558','600530','600498','000687','603559','000609','002572','002206','600979','300655','600516','000039','000615','600558',\
                       '002701','300668','603338','002815','300384','002201','002503','000616','600458','300166','000931','000623','001696','300078','002910','000408',\
                       '000852','600146','300163','000509','603039','600785','600156','000719','600812','603767','002347','002666','600257','002489','002291','002793',\
                       '000615','300760','300123','600814','000860','600558','600530','000687','002572','002206','000039','300762']
        self.logger.info("all code list length:%s" % len(failed_list))
        if cdate is None:
            cfunc = partial(_set_stock_info, cdate, bonus_info, index_info)
            return process_concurrent_run(cfunc, failed_list, num = 5)
        else:
            cfunc = partial(_set_stock_info, cdate, bonus_info, index_info)
            succeed = True
            if not process_concurrent_run(cfunc, failed_list, num = 500):
                succeed = False
            return succeed
            #start_date = get_day_nday_ago(cdate, num = 1, dformat = "%Y-%m-%d")
            #for mdate in get_dates_array(start_date, cdate, asending = True):
            #    if self.cal_client.is_trading_day(mdate):
            #        cfunc = partial(_set_stock_info, mdate, bonus_info, index_info)
            #        if not process_concurrent_run(cfunc, failed_list, num = 500):
            #            succeed = False

    def init_industry_info(self, cdate):
        def _set_industry_info(cdate, code_id):
            return (code_id, CIndex(code_id).set_k_data(cdate))
        df = self.industry_info_client.get()
        if cdate is None:
            cfunc = partial(_set_industry_info, cdate)
            return concurrent_run(cfunc, df.code.tolist(), num = 5)
        else:
            succeed = True
            start_date = get_day_nday_ago(cdate, num = 30, dformat = "%Y-%m-%d")
            for mdate in get_dates_array(start_date, cdate, asending = True):
                if self.cal_client.is_trading_day(mdate):
                    cfunc = partial(_set_industry_info, mdate)
                    if not concurrent_run(cfunc, df.code.tolist(), num = 5):
                        succeed = False
            return succeed

    def init_yesterday_hk_info(self, cdate):
        succeed = True
        for data in ((ct.SH_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL), (ct.SZ_MARKET_SYMBOL, ct.HK_MARKET_SYMBOL)):
            if not self.connect_client.set_market(data[0], data[1]):
                self.logger.error("connect_client for %s failed" % data)
                succeed = False
                continue

            if not self.connect_client.update(cdate):
                succeed = False

            self.connect_client.close()
            self.connect_client.quit()
        kill_process("zygote")
        kill_process("defunct")
        kill_process("show-component-extension-options")
    
        hc = HkexCrawler()
        return hc.run() and succeed

    def get_concerned_index_codes(self):
        index_codes = list(ct.INDEX_DICT.keys())
        index_codes.append('880883')
        return index_codes

    def init_index_components_info(self, cdate = None):
        if cdate is None: cdate = datetime.now().strftime('%Y-%m-%d')
        def _set_index_info(code_id):
            if code_id in self.index_objs:
                _obj = self.index_objs[code_id] 
            else:
                _obj = CIndex(code_id) if code_id in list(ct.INDEX_DICT.keys()) else TdxFgIndex(code_id)
            return (code_id, _obj.set_components_data(cdate))
        index_codes = self.get_concerned_index_codes()
        return concurrent_run(_set_index_info, index_codes, num = 10)

    def set_bull_stock_ratio(self, cdate, num = 10):
        def _set_bull_stock_ratio(code_id):
            return (code_id, BullStockRatio(code_id).update(cdate, num))
        index_codes = self.get_concerned_index_codes()
        return concurrent_run(_set_bull_stock_ratio, index_codes, num = num)

    def init_tdx_index_info(self, cdate = None):
        def _set_index_info(cdate, code_id):
            try:
                if code_id in self.index_objs:
                    _obj = self.index_objs[code_id] 
                else:
                    _obj = CIndex(code_id) if code_id in list(ct.INDEX_DICT.keys()) else TdxFgIndex(code_id)
                return (code_id, _obj.set_k_data(cdate))
            except Exception as e:
                self.logger.error(e)
                return (code_id, False)
        index_code_list = self.get_concerned_index_codes()
        if cdate is None:
            cfunc = partial(_set_index_info, cdate)
            return concurrent_run(cfunc, index_code_list, num = 5)
        else:
            succeed = True
            start_date = get_day_nday_ago(cdate, num = 30, dformat = "%Y-%m-%d")
            for mdate in get_dates_array(start_date, cdate, asending = True):
                if self.cal_client.is_trading_day(mdate):
                    cfunc = partial(_set_index_info, mdate)
                    if not concurrent_run(cfunc, index_code_list, num = 5):
                        succeed = False
            return succeed

    def download_and_extract(self, cdate):
        try:
            if not download(ct.ZIP_DIR, cdate): return False
            list_files = os.listdir(ct.ZIP_DIR)
            for filename in list_files:
                if not filename.startswith('.'):
                    file_path = os.path.join(ct.ZIP_DIR, filename)
                    if os.path.exists(file_path):
                        unzip(file_path, ct.TIC_DIR)
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def scrawler(self, sleep_time):
        schedule.every().monday.do(start_spider)
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                self.logger.info(e)
            gevent.sleep(sleep_time)
 
if __name__ == '__main__':
    #from cmysql import CMySQL
    #mysql_client = CMySQL(dbinfo = ct.DB_INFO)
    #mysql_client.delete_db(ct.RINDEX_STOCK_INFO_DB)
    #import sys
    #sys.exit(0)
    #for code in IndustryInfo().get().code.tolist():
    #    print(code)
    #    mysql_client.delete_db('i%s' % code)
    #for code in CStockInfo().get().code.tolist():
    #    print(code)
    #    mysql_client.delete_db('s%s' % code)
    #import sys
    #sys.exit(0)

    kill_process("google-chrome")
    kill_process("renderer")
    kill_process("Xvfb")
    kill_process("zygote")
    kill_process("defunct")
    kill_process("show-component-extension-options")

    dm = DataManager()
    dm.logger.info("start compute!")
    dm.bootstrap(exec_date = '2019-03-11')
    #mdate = datetime.now().strftime('%Y-%m-%d')
    #mdate = '2019-02-26'
    dm.logger.info("end compute!")
