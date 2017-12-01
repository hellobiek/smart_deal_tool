#coding=utf-8
from __future__ import unicode_literals
import sys
import time
import datetime
import json
import codecs
import urllib2
import numpy as np
import pandas as pd
import tushare as ts
from datetime import datetime
from pandas import Series,DataFrame
from common import create_table,get_all_tables,_fprint,delta_days,get_market_name,trace_func,gint
from mysql import set,get,get_hist_data
from sqlalchemy import create_engine
from const import DB_NAME,DB_USER,DB_PASSWD,DB_HOSTNAME,UTF8,DB_INFO,STOCK_LIST,INDEX_LIST,REAL_INDEX_LIST,AVERAGE_INDEX_LIST
from const import MARKET_SH,MARKET_SZ,MARKET_CYB,SZ50,HS300,ZZ500,MSCI,MARKET_ALL,SQL,SLEEP_INTERVAL,START_DATE
from log import getLogger

pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('max_rows', 200)
logger = getLogger(__name__)

class StockManager:
    def __init__(self):
        self.log = logger
        self.engine = create_engine("mysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8" % DB_INFO)
        self.tables = get_all_tables(DB_USER,DB_PASSWD,DB_NAME,DB_HOSTNAME)
    
    @trace_func(log = logger)
    def init(self):
        if self.is_collecting_time():
            self.init_trading_day()
            self.init_concept()
            self.init_stock_basic_info()
            self.init_index_info()
            self.init_realtime_index_info()
            self.init_average_table_index()
            self.init_realtime_stock_info()
            self.init_realtime_static_info() 
            self.init_trading_info()
            self.init_constituent_stock()

    @trace_func(log = logger)
    def init_constituent_stock(self):
        table = 'constituent'
        sql = 'create table if not exists `%s`(name varchar(20),code varchar(5000))' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, sql):
                raise Exception("create table %s failed" % table)
        df_concept_dict = {}
        for key in STOCK_LIST:
            df_concept_dict[key] = json.dumps(self.init_code_list(key))
        old_df_concept = get(self.engine, SQL % table)
        new_df_concept = DataFrame({'name':df_concept_dict.keys(),'code':df_concept_dict.values()})
        if new_df_concept is not None:
            df_concept = old_df_concept.append(new_df_concept)
            df_concept = df_concept.drop_duplicates()
            set(self.engine, df_concept, str(table))

    @trace_func(log = logger)
    def init_code_list(self, dtype="SZ50"):
        if dtype == "SZ50":
            return list(ts.get_sz50s().code.str.decode('utf-8'))
        elif dtype == "HS300":
            return list(ts.get_hs300s().code.str.decode('utf-8'))
        elif dtype == "ZZ500":
            return list(ts.get_zz500s().code.str.decode('utf-8'))
        else:
            with codecs.open('concepts/msci.json', 'r', 'utf-8') as f:
                return json.load(f)

    @trace_func(log = logger)
    def get_average_price(self, _code, market=MARKET_ALL, _date=None):
        _date = datetime.now().strftime('%Y-%m-%d') if _date is None else _date
        stock_infos = self.get_classified_stocks(market)
        total_data = None
        for _index, code_id in stock_infos['code'].iteritems():
            tmp_data = get_hist_data(self.engine, str(code_id)).query('date=="%s"' % _date)
            if tmp_data is not None:
                tmp_data['code'] = code_id
                tmp_data = tmp_data.reset_index(drop=False)
                total_data = tmp_data if _index == 0 else total_data.append(tmp_data).drop_duplicates()
        num = 0
        open_price = 0
        close_price = 0
        low_price = 0
        high_price = 0
        volume = 0
        for index, _row in total_data.iterrows():
            low_price += _row['low']
            high_price += _row['high']
            open_price += _row['open']
            close_price += _row['close']
            volume += _row['volume']
            num += 1
        open_av_price = 0 if num == 0 else open_price/num 
        close_av_price = 0 if num == 0 else close_price/num 
        low_av_price = 0 if num == 0 else low_price/num 
        high_av_price = 0 if num == 0 else high_price/num 
        volume_av_price = 0 if num == 0 else volume/num
        return DataFrame({'code':[_code],'date':[_date],'open':[open_av_price],'close':[close_av_price],'high':[high_av_price],'volume':[volume_av_price]})  

    @trace_func(log = logger)
    def get_classified_stocks(self, type_name=MARKET_ALL):
        table = "info"
        df = get(self.engine, SQL % table)
        df = df[['code', 'name', 'timeToMarket', 'cName', 'totals', 'outstanding', 'industry', 'area']]
        df['outstanding'] = df['outstanding'] * 1000000
        if type_name == MARKET_SH:
            df = df.ix[df.code.str[0] == '6']
        elif type_name == MARKET_CYB: 
            df = df.ix[df.code.str[0] == '3']
        elif type_name == MARKET_SZ:
            df = df.ix[df.code.str[0] == '0']
        elif type_name == SZ50:
            df = df.ix[df.code.isin(self.get_code_list("SZ50"))]
        elif type_name == HS300:
            df = df.ix[df.code.isin(self.get_code_list("HS300"))]
        elif type_name == ZZ500:
            df = df.ix[df.code.isin(self.get_code_list("ZZ500"))]
        elif type_name == MSCI:
            df = df.ix[df.code.isin(self.get_code_list("MSCI"))]
        else:
            pass
        return df.sort_values('code').reset_index(drop=True)

    @trace_func(log = logger)
    def get_code_list(self, dtype):
        table = "constituent"
        df = get(self.engine, SQL % table)
        return json.loads(df[df.name == dtype].code.values[0])

    @trace_func(log = logger)
    def get_concepts(self):
        table = "concept"
        df = get(self.engine, SQL % table)
        return df.sort_values('code').reset_index(drop=True)

    @trace_func(log = logger)
    def is_code_exists(self, code_id):
        table = "info"
        stock_info = get(self.engine, SQL % table)
        stock_info = stock_info[['code','timeToMarket']]
        return len(stock_info[stock_info.code == code_id].index.tolist())

    @trace_func(log = logger)
    def is_after_release(self, code_id, _date):
        table = "info"
        stock_info = get(self.engine, SQL % table)
        stock_info = stock_info[['code','timeToMarket']]
        _index = stock_info[stock_info.code == code_id].index.tolist()[0]
        time2Market = stock_info.loc[_index, 'timeToMarket']
        if time2Market:
            t = time.strptime(str(time2Market), "%Y%m%d")
            y,m,d = t[0:3]
            time2Market = datetime(y,m,d)
            return (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0
        return False

    @trace_func(log = logger)
    def is_trading_day(self, _date):
        table = "calendar"
        stock_dates_df = get(self.engine, SQL % table)
        return stock_dates_df.query('calendarDate=="%s"' % _date).isOpen.values[0] == 1

    @trace_func(log = logger)
    def get_trading_day(self):
        table = 'calendar'
        return get(self.engine, SQL % table) 

    @trace_func(log = logger)
    def get_concpet(self):
        table = 'concept'
        return get(self.engine, SQL % table)

    @trace_func(log = logger)
    def get_stock_basic_info(self):
        table = 'info'
        return get(self.engine, SQL % table)

    @trace_func(log = logger)
    def init_trading_day(self):
        table = 'calendar'
        sql = 'create table if not exists `%s`(calendarDate varchar(10),isOpen int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, sql):
                raise Exception("create table %s failed" % table)
        old_trading_day = get(self.engine, SQL % table) 
        new_trading_day = ts.trade_cal()
        if new_trading_day is not None:
            trading_day = pd.merge(old_trading_day,new_trading_day,how='outer')
            set(self.engine, trading_day, str(table))

    @trace_func(log = logger)
    def init_trading_info(self): 
        stock_info = self.get_stock_basic_info()
        for code_index,code_id in stock_info['code'].iteritems():
            self.log.info("stock id:%s" % code_id)
            mysql = 'create table if not exists `%s`(date varchar(10),\
                                                    open float,\
                                                    high float,\
                                                    close float,\
                                                    low float,\
                                                    volume float)' % code_id
            mysql_5 = 'create table if not exists %s_5(date varchar(10),\
                                                    open float,\
                                                    high float,\
                                                    close float,\
                                                    low float,\
                                                    volume float)' % code_id
            if code_id not in self.tables: 
                if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, mysql):
                    raise Exception("create table %s failed" % code_id)
            if code_id not in self.tables: 
                if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, mysql_5):
                    raise Exception("create table 5 minute %s failed" % code_id)
            old_data = get_hist_data(self.engine, str(code_id))
            new_data = ts.get_k_data(code_id, retry_count = 10)
            if new_data is not None:
                data = old_data.append(new_data)
                data = data.drop_duplicates()
                set(self.engine, data, str(code_id))
            old_data_5 = get_hist_data(self.engine, "%s_5" % code_id)
            new_data_5 = ts.get_k_data(code_id, retry_count = 10, ktype='5')
            if new_data_5 is not None:
                data_5 = old_data_5.append(new_data_5)
                data_5 = data_5.drop_duplicates()
                set(self.engine, data_5, "%s_5" % code_id)

    @trace_func(log = logger)
    def init_concept(self):
        table = 'concept'
        sql = 'create table if not exists %s(cName varchar(50),code varchar(5000))' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, sql):
                raise Exception("create table %s failed" % table)
        df_concept_dict = {}
        concept_obj = {}
        with codecs.open('concepts/concepts.json', 'r', 'utf-8') as f:
            concept_obj = json.load(f)
        for key in concept_obj:
            df_concept_dict[key] = json.dumps(concept_obj[key])
        old_df_concept = get(self.engine, SQL % table)
        new_df_concept = DataFrame({'cName':df_concept_dict.keys(),'code':df_concept_dict.values()})
        if new_df_concept is not None:
            df_concept = old_df_concept.append(new_df_concept)
            df_concept = df_concept.drop_duplicates()
            set(self.engine, df_concept, str(table))

    @trace_func(log = logger)
    def is_sub_new_stock(self, time2Market, timeLimit = 365):
        if time2Market == '0': #for stock has not benn in market
            return False
        if time2Market:
            t = time.strptime(time2Market, "%Y%m%d")
            y,m,d = t[0:3]
            time2Market = datetime(y,m,d)
            return (datetime.today()-time2Market).days < timeLimit
        return False

    @trace_func(log = logger)
    def init_realtime_index_info(self):
        table = 'realtime_indexes'
        text = 'create table if not exists %s(name varchar(20),\
                                              code varchar(6),\
                                              open float,\
                                              pre_close float,\
                                              price float,\
                                              high float,\
                                              low float,\
                                              volume float,\
                                              p_change float,\
                                              date varchar(20),\
                                              time varchar(20),\
                                              amount float)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)

    @trace_func(log = logger)
    def init_average_table_index(self):
        for tname in AVERAGE_INDEX_LIST:
            text = 'create table if not exists %s(date varchar(10),\
                                                  open float,\
                                                  close float,\
                                                  high float,\
                                                  low float,\
                                                  volume float,\
                                                  code varchar(10))' % tname
            if tname not in self.tables: 
                if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                    raise Exception("create table %s failed" % tname)

    @trace_func(log = logger)
    def init_index_info(self):
        for tname in INDEX_LIST:
            text = 'create table if not exists %s(date varchar(10),\
                                                  open float,\
                                                  close float,\
                                                  high float,\
                                                  low float,\
                                                  volume float,\
                                                  code varchar(10))' % tname
            if tname not in self.tables: 
                if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                    raise Exception("create table %s failed" % tname)
            df_all = get(self.engine, SQL % tname)
            new_data = ts.get_k_data(tname.split('_')[0], index=True)
            if not new_data.empty:
                for index,row in df_all.iterrows():
                    for column in df_all.columns:
                        if isinstance(df_all[column][index], str):
                            df_all[column] = df_all[column].str.decode('utf-8')
                for index,row in new_data.iterrows():
                    for column in new_data.columns:
                        if isinstance(new_data[column][index], str):
                            new_data[column] = new_data[column].str.decode('utf-8')
                df_all = df_all.append(new_data, ignore_index=True)
                df_all = df_all.drop_duplicates(subset = 'date')
            set(self.engine,df_all,tname)

    @trace_func(log = logger)
    def init_realtime_stock_info(self):
        table = 'realtime_stocks'
        text = 'create table if not exists %s(date varchar(25),\
                                              name varchar(20),\
                                              code varchar(10),\
                                              open float,\
                                              pre_close float,\
                                              price float,\
                                              high float,\
                                              low float,\
                                              bid float,\
                                              ask float,\
                                              volume float,\
                                              amount float,\
                                              b1_v int,\
                                              b1_p float,\
                                              b2_v int,\
                                              b2_p float,\
                                              b3_v int,\
                                              b3_p float,\
                                              b4_v int,\
                                              b4_p float,\
                                              b5_v int,\
                                              b5_p float,\
                                              a1_v int,\
                                              a1_p float,\
                                              a2_v int,\
                                              a2_p float,\
                                              a3_v int,\
                                              a3_p float,\
                                              a4_v int,\
                                              a4_p float,\
                                              a5_v int,\
                                              a5_p float,\
                                              time varchar(20),\
                                              turnover float,\
                                              p_change float,\
                                              outstanding float,\
                                              limit_dowm_time varchar(20),\
                                              limit_up_time varchar(20))' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)

    @trace_func(log = logger)
    def init_realtime_static_info(self):
        table = 'daily_statics'
        text = 'create table if not exists %s(date varchar(20),\
                                              time varchar(20),\
                                              neg_10 int,\
                                              neg_9 int,\
                                              neg_8 int,\
                                              neg_7 int,\
                                              neg_6 int,\
                                              neg_5 int,\
                                              neg_4 int,\
                                              neg_3 int,\
                                              neg_2 int,\
                                              neg_1 int,\
                                              zero  int,\
                                              pos_1 int,\
                                              pos_2 int,\
                                              pos_3 int,\
                                              pos_4 int,\
                                              pos_5 int,\
                                              pos_6 int,\
                                              pos_7 int,\
                                              pos_8 int,\
                                              pos_9 int,\
                                              pos_10 int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)

    @trace_func(log = logger)
    def init_stock_basic_info(self):
        table = 'info'
        text = 'create table if not exists %s(code varchar(10),\
                                              name varchar(10),\
                                              industry varchar(20),\
                                              cName varchar(50),\
                                              area varchar(20),\
                                              pe float,\
                                              outstanding float,\
                                              totals float,\
                                              totalAssets float,\
                                              fixedAssets float,\
                                              liquidAssets float,\
                                              reserved float,\
                                              reservedPerShare float,\
                                              esp float,\
                                              bvps float,\
                                              pb float,\
                                              timeToMarket varchar(20),\
                                              undp float,\
                                              perundp float,\
                                              rev float,\
                                              profit float,\
                                              gpr float,\
                                              npr float,\
                                              limitUpNum int,\
                                              limitDownNum int,\
                                              holders int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)
        old_df_all = get(self.engine, SQL % table)
        for index,row in old_df_all.iterrows():
            for column in old_df_all.columns:
                if isinstance(old_df_all[column][index], str):
                    old_df_all[column] = old_df_all[column].str.decode('utf-8')
        df = ts.get_stock_basics()
        concepts = self.get_concpet()
        if df is not None:
            df = df.reset_index().rename_axis({'index':'code'},axis="columns")
            for index,_ in df.iterrows():
                for column in df.columns:
                    if isinstance(df[column][index], str):
                        df[column] = df[column].str.decode('utf-8')
            df['limitUpNum'] = 0
            df['limitDownNum'] = 0
            df_concept_dict = {}
            for index, code_id_str in concepts['code'].iteritems():
                code_id_list = json.loads(code_id_str)
                concept_name = concepts['cName'][index]
                for code_id in code_id_list:
                    if code_id not in df_concept_dict:
                        df_concept_dict[code_id] = []
                    if concept_name not in df_concept_dict[code_id]:
                        df_concept_dict[code_id].append(concept_name)
            for key in df_concept_dict:
                df_concept_dict[key] = json.dumps(df_concept_dict[key])
            df_concept = DataFrame(list(df_concept_dict.items()), columns=['code', 'cName'])
            df_all = pd.merge(df,df_concept,how='left',on=['code'])
            df_all.reset_index(drop=True)
            if not old_df_all.empty:
                df_all = old_df_all.append(df_all, ignore_index=True)
                df_all = df_all.drop_duplicates(subset = 'code')
            set(self.engine,df_all,table)

    @trace_func(log = logger)
    def get_post_trading_day(self, _date):
        table = 'calendar'
        df = get(self.engine, SQL % table)
        _index = df[df.calendarDate == _date].index.tolist()[0]
        if _index > 0:
            _tindex = _index
            while _tindex < len(df):
                _tindex += 1
                if df['isOpen'][_tindex] == 1:
                    return df['calendarDate'][_tindex]

    @trace_func(log = logger)
    def get_pre_trading_day(self, _date):
        table = 'calendar'
        df = get(self.engine, SQL % table)
        _index = df[df.calendarDate == _date].index.tolist()[0]
        if _index > 0:
            _tindex = _index
            while _tindex > 0:
                _tindex -= 1
                if df['isOpen'][_tindex] == 1:
                    return df['calendarDate'][_tindex]

    @trace_func(log = logger)
    def get_highest_time(self, code_id, pre_close_price, sdate):
        data_info = get_hist_data(self.engine, "%s_5" % code_id)
        data_info = df[df.date == sdate] 
        if data_info is not None:
            data_info = data_info.reset_index(drop=False)
            tmp_df = data_info[['close','date']].sort_values(by = 'date', ascending = True)
            for index, cur_price in tmp_df['close'].iteritems():
                total_p_change = (cur_price - pre_close_price) * 100 / pre_close_price
                if total_p_change > 9.8:
                    return tmp_df['date'][index]

    @trace_func(log = logger)
    def get_realtime_index_info(self):
        table = "realtime_indexes"
        return get(self.engine, SQL % table)

    @trace_func(log = logger)
    def set_realtime_index_info(self):
        table = "realtime_indexes"
        all_info = ts.get_realtime_quotes(REAL_INDEX_LIST)
        all_info = all_info[['name','code','open','pre_close','price','high','low','volume','date','time','amount']]
        all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
        all_info['date'] = datetime.now().strftime('%Y-%m-%d')
        set(self.engine, all_info, table)

    @trace_func(log = logger)
    def get_average_index_info(self, tname):
        return get(self.engine, SQL % tname)

    @trace_func(log = logger)
    def set_average_index_info(self):
        _today = datetime.now().strftime('%Y-%m-%d')
        for tname in AVERAGE_INDEX_LIST:
            code = tname.split('_')[0]
            existed_data = self.get_average_index_info(tname)
            existed_data = existed_data[existed_data['close'] != 0]
            start_date = START_DATE if 0 == len(existed_data) else self.get_post_trading_day(existed_data['date'][len(existed_data) - 1])
            num_days = delta_days(start_date, _today)
            start_date_dmy_format = time.strftime("%d/%m/%Y", time.strptime(start_date, "%Y-%m-%d"))
            data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
            date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
            total_data = None
            market_name = get_market_name(tname.split('_')[1])
            for _date in date_only_array:
                if self.is_trading_day(_date):
                    tmp_data = self.get_average_price(code, market_name, _date)
                    total_data = tmp_data if total_data is None else total_data.append(tmp_data).drop_duplicates(subset = 'date')
            total_data = existed_data if total_data is None else existed_data.append(total_data)
            set(self.engine,total_data,tname)

    @trace_func(log = logger)
    def get_realtime_stock_info(self):
        table = 'realtime_stocks'
        return get(self.engine, SQL % table)

    @trace_func(log = logger)
    def set_realtime_stock_info(self):
        table = "realtime_stocks"
        all_info = None
        start_index = 0
        stock_infos = self.get_classified_stocks()
        stock_nums = len(stock_infos)
        while start_index < stock_nums:
            end_index = stock_nums - 1 if start_index + 800 > stock_nums else start_index + 800 -1
            stock_codes = stock_infos['code'][start_index:end_index]
            _info = ts.get_realtime_quotes(stock_codes)
            all_info = _info if start_index == 0 else all_info.append(_info)
            start_index = end_index + 1
        all_info['limit_up_time'] = 0
        all_info['limit_down_time'] = 0
        all_info['outstanding'] = stock_infos['outstanding']
        all_info = all_info[(all_info['volume'].astype(float) > 0) & (all_info['outstanding'] > 0)]
        all_info['turnover'] = all_info['volume'].astype(float).divide(all_info['outstanding'])
        all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
        now_time = datetime.now().strftime('%H-%M-%S')
        all_info[all_info["p_change"]>9.9]['limit_up_time'] = now_time
        all_info[all_info["p_change"]<-9.9]['limit_down_time'] = now_time
        set(self.engine, all_info, table)

    @trace_func(log = logger)
    def is_trading_time(self):
        now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (9,24,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (11,31,0)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        aft_open_hour,aft_open_minute,aft_open_second = (13,0,0)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (15,0,0)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        return (mor_open_time < now_time < mor_close_time) or (aft_open_time < now_time < aft_close_time)

    def is_collecting_time(self):
        now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (21,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    @trace_func(log = logger)
    def get_realtime_static_info(self):
        table = 'daily_statics'
        return get(self.engine, SQL % table)

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
    #                                hist_data = get_hist_data(engine, code_id, _date)
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
    #                    hist_data = get_hist_data(self.engine, code_id, _date)
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

    @trace_func(log = logger)
    def set_realtime_static_info(self):
        table = 'daily_statics'
        _date = datetime.now().strftime('%Y-%m-%d')
        if self.is_trading_day(_date):
            if self.is_trading_time():
                data = self.get_realtime_stock_info()
                if data is not None:
                    _mdate = datetime.now().strftime('%Y-%m-%d')
                    _mtime = datetime.now().strftime('%H-%M-%S')
                    _row = [0 for i in xrange(21)]
                    p_change_list = [gint(x) for x in data['p_change'].tolist()]
                    for x in p_change_list:
                        _row[x + 10] += 1
                    df = DataFrame({'time':[_mtime], 'date':[_mdate],'neg_10':[_row[0]],'neg_9':[_row[1]],'neg_8':[_row[2]],'neg_7':[_row[3]],'neg_6':[_row[4]],'neg_5':[_row[5]],'neg_4':[_row[6]],'neg_3':[_row[7]],'neg_2':[_row[8]],'neg_1':[_row[9]],'zero':[_row[10]],'pos_1':[_row[11]],'pos_2':[_row[12]],'pos_3':[_row[13]],'pos_4':[_row[14]],'pos_5':[_row[15]],'pos_6':[_row[16]],'pos_7':[_row[17]],'pos_8':[_row[18]],'pos_9':[_row[19]],'pos_10':[_row[20]]})
                    old_data = self.get_realtime_static_info()
                    old_data = df if old_data is None else old_data.append(df)
                    old_data = old_data.drop_duplicates(subset = ['date','time'])
                    set(self.engine,old_data,table)
