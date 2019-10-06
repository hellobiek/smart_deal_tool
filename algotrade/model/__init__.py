# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import abc
import const as ct
import pandas as pd
from base.cobj import CMysqlObj
from base.clog import getLogger
from ccalendar import CCalendar
from base.cdate import get_dates_array
ORDER_TABLE = 'orders'
ACCOUNT_TABLE = 'accounts'
POSITION_TABLE  = 'positions'
class QModel(CMysqlObj):
    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None, cal_file_path = ct.CALENDAR_PATH):
        super(QModel, self).__init__(code, code, dbinfo, redis_host)
        self.logger = getLogger(__name__)
        self.cal_client = CCalendar(dbinfo = dbinfo, redis_host = redis_host, filepath = cal_file_path)

    @abc.abstractmethod
    def create_table(self, table_name):
        raise NotImplementedError()

    def set_stock_pool(self, mdate):
        if mdate is None: return False
        table_name = self.get_table_name(mdate)
        if not self.is_table_exists(table_name):
            if not self.create_table(table_name):
                self.logger.error("create chip table:{} failed".format(table_name))
                return False

        if self.is_date_exists(table_name, mdate):
            self.logger.debug("existed data for code:{}, date:{}".format(self.code, mdate))
            return True

        df = self.compute_stock_pool(mdate)
        if df.empty: return True
        if self.mysql_client.set(df, table_name):
            return self.redis.sadd(table_name, mdate)
        return False

    @abc.abstractmethod
    def compute_stock_pool(self, mdate):
        raise NotImplementedError()

    @abc.abstractmethod
    def generate_feed(self, start_date, end_date):
        raise NotImplementedError()

    def get_table_name(self, mdate):
        mdates = mdate.split('-')
        return "{}_{}".format(self.code, mdates[0])

    def generate_stock_pool(self, start_date, end_date):
        succeed = True
        date_array = get_dates_array(start_date, end_date)
        for mdate in date_array:
             if self.cal_client.is_trading_day(mdate):
                 if not self.set_stock_pool(mdate):
                     self.logger.error("set {} data for model failed".format(mdate))
                     succeed = False
        return succeed

    def get_stock_pool(self, mdate):
        if mdate is None: return pd.DataFrame()
        table_name = self.get_table_name(mdate)
        if not self.is_date_exists(table_name, mdate): return pd.DataFrame()
        sql = "select * from %s where date=\"%s\"" % (table_name, mdate)
        df = self.mysql_client.get(sql)
        return pd.DataFrame() if df is None else df

    def create_order_table(self, table_name = ORDER_TABLE):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 order_id varchar(50) not null,\
                                                 trd_side varchar(10),\
                                                 order_type varchar(20),\
                                                 order_status varchar(20),\
                                                 code varchar(20),\
                                                 stock_name varchar(50),\
                                                 qty float,\
                                                 price float,\
                                                 create_time varchar(50),\
                                                 updated_time varchar(50),\
                                                 dealt_qty float,\
                                                 dealt_avg_price float,\
                                                 last_err_msg varchar(100),\
                                                 remark varchar(64) not null,\
                                                 PRIMARY KEY(date, order_id))' % table_name 
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def create_account_table(self, table_name = ACCOUNT_TABLE):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 power float not null,\
                                                 total_assets float not null,\
                                                 cash float not null,\
                                                 market_val float not null,\
                                                 frozen_cash float not null,\
                                                 avl_withdrawal_cash float not null,\
                                                 PRIMARY KEY(date))' % table_name 
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def create_position_table(self, table_name = POSITION_TABLE):
        if not self.mysql_client.is_exists(table_name):
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 code varchar(20) not null,\
                                                 position_side varchar(10) not null,\
                                                 stock_name varchar(50) not null,\
                                                 qty float,\
                                                 can_sell_qty float,\
                                                 nominal_price float,\
                                                 cost_price float,\
                                                 cost_price_valid boolean,\
                                                 market_val float,\
                                                 pl_ratio float,\
                                                 pl_ratio_valid	boolean,\
                                                 pl_val	float,\
                                                 pl_val_valid boolean,\
                                                 today_pl_val float,\
                                                 today_buy_qty float,\
                                                 today_buy_val float,\
                                                 today_sell_qty float,\
                                                 today_sell_val float,\
                                                 PRIMARY KEY(date, code))' % table_name 
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def set_account_info(self, mdate, broker):
        account_info = broker.get_accinfo()
        account_info['date'] = mdate
        return self.mysql_client.set(account_info, ACCOUNT_TABLE)

    def get_info(self, table, start, end):
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (table, start, end)
        return self.mysql_client.get(sql)

    def get_account_info(self, start, end):
        return self.get_info(ACCOUNT_TABLE, start, end)

    def get_position_info(self, start, end):
        return self.get_info(POSITION_TABLE, start, end)

    def get_history_order_info(self, start, end):
        return self.get_info(ORDER_TABLE, start, end)

    def set_position_info(self, mdate, broker):
        position_info = broker.get_postitions()
        position_info['date'] = mdate
        return self.mysql_client.set(position_info, POSITION_TABLE)

    def set_history_order_info(self, mdate, broker):
        order_info = broker.get_history_orders(start = mdate, end = mdate)
        order_info['date'] = mdate
        return self.mysql_client.set(order_info, ORDER_TABLE)
