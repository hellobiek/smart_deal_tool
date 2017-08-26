# encoding: utf-8
import json,sys
import tushare as ts
import pandas as pd
from common import create_table,get_all_tables
from mysql import set,get
from pandas import DataFrame
from sqlalchemy import create_engine
from const import DB_NAME,DB_USER,DB_PASSWD,DB_HOSTNAME,UTF8,SQL
class StockManager:
    def __init__(self):
        self.engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8'%(DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
        self.tables = get_all_tables(DB_USER,DB_PASSWD,DB_NAME,DB_HOSTNAME)
        self.trading_day = self.init_trading_day()
        self.stock_info = self.init_stock_basic_info()
        self.forward_adjust_info = self.init_forward_adjusted_price()
        self.concepts = self.init_concept_from_ts()
        self.init_daily_info()
        self.init_trading_info()
        self.init_daily_statics_info()

    def init_trading_day(self):
        table = 'calendar'
        sql = 'create table if not exists `%s`(calendarDate varchar(10),isOpen int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, sql):
                raise Exception("create table %s failed" % table)
        trading_day = ts.trade_cal()
        if trading_day is not None:
            set(self.engine, trading_day, str(table))
        return get(self.engine, SQL % table)

    def init_forward_adjusted_price(self):
        _today = datetime.now().strftime('%Y-%m-%d')
        start_date = str(datetime.datetime.today().date() + datetime.timedelta(-1))
        end_date = str(datetime.datetime.today().date())
        stock_infos = get_classified_stocks(market)
        total_data = None
        table = 'forward_adjusted_price'
        sql = 'create table if not exists `%s`(date varchar(10),\
                                               open float,\
                                               high float,\
                                               close float,\
                                               low float,\
                                               volume float,\
                                               code varchar(8))' % table
        if is_trading_day(_today):
            for _index, code_id in stock_infos['code'].iteritems():
                tmp_data = ts.get_k_data(code_id,start=start_date, end=end_date, autype='qfq')
                tmp_data['code'] = code_id
                tmp_data = tmp_data.reset_index(drop=False)
                total_data = tmp_data if _index == 0 else pd.concat([total_data,tmp_data],ignore_index=True).drop_duplicates()
            set(self.engine, total_data, table)
        return get(self.engine, SQL % table)

    def init_trading_info(self): 
        for code_index,code_id in self.stock_info['code'].iteritems():
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
            print code_index,code_id
            if code_id not in self.tables: 
                if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, mysql):
                    raise Exception("create table %s failed" % code_id)
            data = ts.get_k_data(code_id, retry_count = 10)
            if data is not None:
                set(self.engine, data.reset_index(), str(code_id))
            data_5 = ts.get_k_data(code_id, retry_count = 10, ktype='5')
            if data_5 is not None:
                set(self.engine, data_5.reset_index(), "%s_5" % code_id)

    def init_concept_from_ts(self):
        table = 'concept'
        sql = 'create table if not exists %s(c_name varchar(50),code varchar(5000))' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, sql):
                raise Exception("create table %s failed" % table)
        df_concept = ts.get_concept_classified()
        df_concept = df_concept[['code', 'c_name']]
        df_concept_dict = {}
        for index, code_id in df_concept['code'].iteritems():
            concept_name = df_concept['c_name'][index]
            if concept_name in df_concept_dict:
                if not code_id in df_concept_dict:
                    df_concept_dict[concept_name].append(code_id)
            else:
                df_concept_dict[concept_name] = []
                df_concept_dict[concept_name].append(code_id)
        concept_obj = {} 
        with open("concepts/concepts.json") as f:
            concept_obj = json.load(f)
        for key, value in concept_obj.items():
            if key in df_concept_dict:
                if value not in df_concept_dict[key]:
                    df_concept_dict[key].extend(value)
            else:
                df_concept_dict[key] = value
        for key in df_concept_dict:
            df_concept_dict[key] = json.dumps(df_concept_dict[key], ensure_ascii=False)
        df_concept = DataFrame({'c_name':df_concept_dict.keys(),'code':df_concept_dict.values()})
        set(self.engine, df_concept, table)
    	return get(self.engine, SQL % table)

    def init_daily_info(self):
        table = 'today'
        text = 'create table if not exists %s(date varchar(25),\
                                              name varchar(20),\
                                              code varchar(10),\
                                              turnover float,\
                                              p_change float,\
                                              price float,\
                                              limit-up-time varchar(20))' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)

    def init_daily_statics_info(self):
        table = 'daily_statics'
        text = 'create table if not exists %s(neg_9 int,\
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
                                              pos_9 int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)

    def init_stock_basic_info(self):
        table = 'info'
        text = 'create table if not exists %s(code int,\
                                              name varchar(20),\
                                              industry varchar(20),\
                                              c_name varchar(100),\
                                              area varchar(20),\
                                              pe float,\
                                              outstanding float,\
                                              totals float,\
                                              totalAssets float,\
                                              fixedAssets float,\
                                              reserved float,\
                                              reservedPerShare float,\
                                              esp float,\
                                              bvps float,\
                                              pb float,\
                                              timeToMarket int,\
                                              undp float,\
                                              perundp float,\
                                              rev float,\
                                              profit float,\
                                              gpr float,\
                                              npr float,\
                                              limit-up-num int,\
                                              continuous-limit-up-num int,\
                                              holders int)' % table
        if table not in self.tables: 
            if not create_table(DB_USER, DB_PASSWD, DB_NAME, DB_HOSTNAME, text):
                raise Exception("create table %s failed" % table)
        df = ts.get_stock_basics()
        df = df.reset_index().rename_axis({'index':'code'},axis="columns")
        df_concept = ts.get_concept_classified()
        df_concept = df_concept[['code', 'c_name']]
        df_concept_dict = {}
        for index, code_id in df_concept['code'].iteritems():
            concept_name = df_concept['c_name'][index]
            if code_id in df_concept_dict:
                if not concept_name in df_concept_dict:
                    df_concept_dict[code_id].append(concept_name)
            else:
                df_concept_dict[code_id] = []
                df_concept_dict[code_id].append(concept_name)
        for key in df_concept_dict:
            df_concept_dict[key] = json.dumps(df_concept_dict[key],ensure_ascii=False)
        df_concept = DataFrame({'code':df_concept_dict.keys(),'c_name':df_concept_dict.values(), 'limit-up-num': 0, 'continuous-limit-up-num': 0})
        df_all = pd.merge(df,df_concept,how='left',on=['code'])
        df_all.reset_index(drop=True)
        set(self.engine,df_all,table)
    	return get(self.engine, SQL % table)
