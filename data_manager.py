#coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
from gevent.event import AsyncResult
import sys
import time
import json
import datetime
import ccalendar
import cstock
import cstock_info
import cdelisted
import combination
import combination_info
import const as ct
import pandas as pd
import tushare as ts
from pandas import DataFrame
from log import getLogger
from datetime import datetime
from common import trace_func,is_trading_time,_fprint

pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('max_rows', 200)
logger = getLogger(__name__)

class DataManager:
    @trace_func(log = logger)
    def __init__(self, dbinfo, stock_info_table, combination_info_table, calendar_table, delisted_table):
        self.cal_client = ccalendar.CCalendar(dbinfo, calendar_table)
        self.comb_info_client = combination_info.CombinationInfo(dbinfo, combination_info_table)
        self.stock_info_client = cstock_info.CStockInfo(dbinfo, stock_info_table) 
        self.delisted_info_client = cdelisted.CDelisted(dbinfo, delisted_table)
        self.evt = AsyncResult()
  
    @trace_func(log = logger)
    def is_collecting_time(self, now_time = None):
        return True
        if now_time is None:now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (17,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (23,0,1)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    @trace_func(log = logger)
    def prepare(self, sleep_time):
        while True:
            if not self.cal_client.is_trading_day():
                time.sleep(ct.LONG_SLEEP_TIME)
            else:
                if self.is_collecting_time():
                    self.init()
                time.sleep(sleep_time)

    @trace_func(log = logger)
    def init(self):
        #create info table for trading data
        self.cal_client.init()
        self.comb_info_client.init()
        self.stock_info_client.init()
        self.delisted_info_client.init()
        #init trading data
        self.init_trading_info(ct.C_STOCK)
        self.init_trading_info(ct.C_INDEX)

    @trace_func(log = logger)
    def init_trading_info(self, h_type = ct.C_STOCK):
        trading_info = self.stock_info_client.get() if h_type == ct.C_STOCK else self.comb_info_client.get(h_type)
        for code_index,code_id in trading_info['code'].iteritems():
            if ct.C_STOCK == h_type:
                obj = cstock.CStock(self.stock_info_client.dbinfo, code_id, self.stock_info_client.table)
            else:
                obj = combination.Combination(self.comb_info_client.dbinfo, code_id, self.comb_info_client.table)
            obj.init()

    @trace_func(log = logger)
    def run(self, dtype, sleep_time):
        while True:
            if not self.cal_client.is_trading_day(): 
                time.sleep(ct.LONG_SLEEP_TIME)
            else:
                if is_trading_time(): 
                    self.collect_realtime_info(dtype)
                #time.sleep(sleep_time)

    @trace_func(log = logger)
    def get_all_info_from_remote(self, stock_infos):
        all_info = None
        start_index = 0
        stock_nums = len(stock_infos)
        while start_index < stock_nums:
            end_index = stock_nums - 1 if start_index + 800 > stock_nums else start_index + 800 -1
            stock_codes = stock_infos['code'][start_index:end_index]
            _info = ts.get_realtime_quotes(stock_codes)
            if _info is not None:
                all_info = _info if all_info is None else all_info.append(_info)
            else:
                logger.error("get %s from tushare failed" % stock_codes)
            start_index = end_index + 1
        if all_info is not None:
            all_info['limit_up_time'] = 0
            all_info['limit_down_time'] = 0
            all_info['outstanding'] = stock_infos['outstanding']
            all_info = all_info[(all_info['volume'].astype(float) > 0) & (all_info['outstanding'] > 0)]
            all_info['turnover'] = all_info['volume'].astype(float).divide(all_info['outstanding'])
            all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
            now_time = datetime.now().strftime('%H-%M-%S')
            all_info[all_info["p_change"]>9.9]['limit_up_time'] = now_time
            all_info[all_info["p_change"]<-9.9]['limit_down_time'] = now_time
            self.evt.set(all_info)

    @trace_func(log = logger)
    def collect_realtime_info(self, h_type = ct.C_STOCK):
        trading_info = self.stock_info_client.get() if h_type == ct.C_STOCK else self.comb_info_client.get(h_type)
        if not trading_info.empty and trading_info is not None:
            obj_list = []
            setter_obj = gevent.spawn(self.get_all_info_from_remote, trading_info)
            for _index,code_id in trading_info['code'].iteritems():
                #logger.debug("%s %s is collected, total:%s" % (h_type, code_id, _index + 1))
                if ct.C_STOCK == h_type:
                    obj = cstock.CStock(self.stock_info_client.dbinfo, code_id, self.stock_info_client.table)
                    obj_list.append(gevent.spawn(obj.run, self.evt))
                else:
                    obj = combination.Combination(self.comb_info_client.dbinfo, code_id, self.comb_info_client.table)
                    obj_list.append(gevent.spawn(obj.run))
                if (_index + 1) % ct.QUEUE_SZIE == 0 or (_index + 1) == len(trading_info):
                    if ct.C_STOCK == h_type: obj_list.append(setter_obj)
                    gevent.joinall(obj_list)
                    obj_list = []

    #@trace_func(log = logger)
    #def init_realtime_static_info(self):
    #    table = ct.DAILY_STATIC_TABLE
    #    sql = 'create table if not exists %s(date varchar(20),\
    #                                          time varchar(20),\
    #                                          neg_10 int,\
    #                                          neg_9 int,\
    #                                          neg_8 int,\
    #                                          neg_7 int,\
    #                                          neg_6 int,\
    #                                          neg_5 int,\
    #                                          neg_4 int,\
    #                                          neg_3 int,\
    #                                          neg_2 int,\
    #                                          neg_1 int,\
    #                                          zero  int,\
    #                                          pos_1 int,\
    #                                          pos_2 int,\
    #                                          pos_3 int,\
    #                                          pos_4 int,\
    #                                          pos_5 int,\
    #                                          pos_6 int,\
    #                                          pos_7 int,\
    #                                          pos_8 int,\
    #                                          pos_9 int,\
    #                                          pos_10 int)' % table
    #    if table not in self.tables: 
    #        if not create_table(ct.DB_INFO['user'],ct.DB_INFO['password'],ct.DB_INFO['database'],ct.DB_INFO['host'], sql):
    #            raise Exception("create table %s failed" % table)

    #@trace_func(log = logger)
    #def get_highest_time(self, code_id, pre_close_price, sdate):
    #    data_info = get_k_data(self.engine, "%s_5" % code_id)
    #    data_info = df[df.date == sdate] 
    #    if data_info is not None:
    #        data_info = data_info.reset_index(drop=False)
    #        tmp_df = data_info[['close','date']].sort_values(by = 'date', ascending = True)
    #        for index, cur_price in tmp_df['close'].iteritems():
    #            total_p_change = (cur_price - pre_close_price) * 100 / pre_close_price
    #            if total_p_change > 9.8:
    #                return tmp_df['date'][index]

    #@trace_func(log = logger)
    #def set_average_index_info(self):
    #    _today = datetime.now().strftime('%Y-%m-%d')
    #    for tname in AVERAGE_INDEX_LIST:
    #        code = tname.split('_')[0]
    #        existed_data = self.get_average_index_info(tname)
    #        existed_data = existed_data[existed_data['close'] != 0]
    #        start_date = START_DATE if 0 == len(existed_data) else self.get_post_trading_day(existed_data['date'][len(existed_data) - 1])
    #        num_days = delta_days(start_date, _today)
    #        start_date_dmy_format = time.strftime("%d/%m/%Y", time.strptime(start_date, "%Y-%m-%d"))
    #        data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
    #        date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
    #        total_data = None
    #        market_name = get_market_name(tname.split('_')[1])
    #        for _date in date_only_array:
    #            if self.is_trading_day(_date):
    #                tmp_data = self.get_average_price(code, market_name, _date)
    #                total_data = tmp_data if total_data is None else total_data.append(tmp_data).drop_duplicates(subset = 'date')
    #        total_data = existed_data if total_data is None else existed_data.append(total_data)
    #        set(self.engine,total_data,tname)

    #@trace_func(log = logger)
    #def collect_concept_volume_price(data_times):
    #    table = 'concept'
    #    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    #    concepts = get(engine, SQL % table)
    #    stock_infos = get(engine, SQL % 'info')
    #    pydate_array = data_times.to_pydatetime()
    #    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    #    tables = get_all_tables(DB_USER,DB_PASSWD,DB_NAME,DB_HOSTNAME)
    #    infos = dict()
    #    for _date in date_only_array:
    #        if is_trading_day(_date):
    #            for index,row in concepts.iterrows():
    #                concept_name = row['c_name']
    #                infos[concept_name] = list()
    #                codes = json.loads(row['code'])
    #                for code_id in codes:
    #                    if is_code_exists(code_id):
    #                        if is_after_release(code_id, _date):
    #                            if code_id in tables:
    #                                hist_data = get_k_data(engine, code_id, _date)
    #                                rate = hist_data['p_change']
    #                                if len(rate) > 0 and rate[0] > 5:
    #                                    volume = hist_data['volume'][0]
    #                                    pre_price = hist_data['close'][0] + hist_data['price_change'][0]
    #                                    c_index = stock_infos[stock_infos.code == code_id].index.tolist()[0]
    #                                    code_name = stock_infos.loc[c_index, 'name']
    #                                    up_date = get_highest_time(code_id, _date, pre_price)
    #                                    infos[concept_name].append((code_id, code_name, rate[0], up_date, volume))
    #    return infos

    #@trace_func(log = logger)
    #def set_stock_turnover_info(self, market, data_times):
    #    stock_id_frame = self.get_classified_stocks(market)
    #    stock_ids = list()
    #    stock_names = list()
    #    stock_turnover = list()
    #    stock_volume = list()
    #    stock_concepts = list()
    #    stock_dates = list()
    #    stock_pchanges = list()
    #    pydate_array = data_times.to_pydatetime()
    #    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    #    for _date in date_only_array:
    #        if is_trading_day(_date):
    #            for code_index, code_id in stock_id_frame['code'].iteritems():
    #                if not self.is_sub_new_stock(str(stock_id_frame['timeToMarket'][code_index])):
    #                    turnover = 0
    #                    volume = 0
    #                    stock_name = stock_id_frame['name'][code_index]
    #                    stock_concept = stock_id_frame['c_name'][code_index]
    #                    hist_data = get_k_data(self.engine, code_id, _date)
    #                    if hist_data is not None:
    #                        hist_data = hist_data[hist_data['volume'] > 0]
    #                        hist_data = hist_data[['turnover','volume','p_change']]
    #                        if not hist_data.empty:
    #                            turnover = hist_data['turnover'][0]
    #                            p_change = hist_data['p_change'][0]
    #                            stock_dates.append(_date)
    #                            stock_ids.append(code_id)
    #                            stock_names.append(stock_name)
    #                            volume = hist_data['volume'][0]
    #                            stock_volume.append(volume)
    #                            stock_turnover.append(turnover)
    #                            stock_concepts.append(stock_concept)
    #                            stock_pchanges.append(p_change)
    #    df = DataFrame({'date':stock_dates,'code':stock_ids,'name':stock_names,'turnover':stock_turnover, 'volume': stock_volume,'c_name': stock_concepts,'p_change': stock_pchanges})
    #    set(self.engine,df,table)

    #@trace_func(log = logger)
    #def set_realtime_static_info(self):
    #    table = 'daily_statics'
    #    _date = datetime.now().strftime('%Y-%m-%d')
    #    if self.is_trading_day(_date):
    #        if self.is_trading_time():
    #            data = self.get_realtime_stock_info()
    #            if data is not None:
    #                _mdate = datetime.now().strftime('%Y-%m-%d')
    #                _mtime = datetime.now().strftime('%H-%M-%S')
    #                _row = [0 for i in xrange(21)]
    #                p_change_list = [gint(x) for x in data['p_change'].tolist()]
    #                for x in p_change_list:
    #                    _row[x + 10] += 1
    #                df = DataFrame({'time':[_mtime], 'date':[_mdate],'neg_10':[_row[0]],'neg_9':[_row[1]],'neg_8':[_row[2]],'neg_7':[_row[3]],'neg_6':[_row[4]],'neg_5':[_row[5]],'neg_4':[_row[6]],'neg_3':[_row[7]],'neg_2':[_row[8]],'neg_1':[_row[9]],'zero':[_row[10]],'pos_1':[_row[11]],'pos_2':[_row[12]],'pos_3':[_row[13]],'pos_4':[_row[14]],'pos_5':[_row[15]],'pos_6':[_row[16]],'pos_7':[_row[17]],'pos_8':[_row[18]],'pos_9':[_row[19]],'pos_10':[_row[20]]})
    #                old_data = self.get_realtime_static_info()
    #                old_data = df if old_data is None else old_data.append(df)
    #                old_data = old_data.drop_duplicates(subset = ['date','time'])
    #                set(self.engine,old_data,table)

if __name__ == '__main__':
    dm = DataManager(ct.DB_INFO, ct.STOCK_INFO_TABLE, ct.COMBINATION_INFO_TABLE, ct.CALENDAR_TABLE, ct.DELISTED_INFO_TABLE)
    dm.run(ct.C_STOCK, 1)
