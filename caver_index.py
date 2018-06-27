#coding=utf-8
import time
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from cmysql import CMySQL
import chalted 
import cdelisted as cdelist 
import ccalendar as ccal
import cstock_info as cs_info
import combination_info as cm_info
from combination import Combination  
from log import getLogger
from pandas import DataFrame
from common import trace_func, delta_days, _fprint

logger = getLogger(__name__)

class CAverIndex(Combination):
    @trace_func(log = logger)
    def __init__(self, name, code, dbinfo, stock_info_table, combination_info_table, calendar_info_table, delisted_info_table, halted_info_table):
        Combination.__init__(self, dbinfo, code, combination_info_table)
        self.stock_info_client = cs_info.CStockInfo(dbinfo, stock_info_table)
        self.calendar_info_client = ccal.CCalendar(dbinfo, calendar_info_table)
        self.cdelist_info_client = cdelist.CDelisted(dbinfo, delisted_info_table)
        self.chalt_info_client = chalted.CHalted(dbinfo, halted_info_table, stock_info_table, calendar_info_table)

    @trace_func(log = logger)
    def init(self):
        _today = datetime.now().strftime('%Y-%m-%d')
        for data_type, data_table in self.data_type_dict.items():
            old_data = self.get_k_data()
            new_data = self.compute()
            if not new_data.empty:
                data = old_data.append(new_data)
                data = data.drop_duplicates(subset = 'date').reset_index(drop = True)
                self.mysql_client.set(data, data_table)

    @trace_func(log = logger)
    def run(self):
        new_data = self.compute(dtype='realtime')
        if not new_data.empty:
            data = old_data.append(new_data)
            data = data.drop_duplicates(subset = 'date').reset_index(drop = True)
            self.mysql_client.set(data, data_table)

    @trace_func(log = logger)
    def compute(self, _date = None, dtype = 'D'):
        num = 0
        total_data = None
        _date = datetime.now().strftime('%Y-%m-%d') if _date is None else _date
        _date = '2018-05-25'
        stock_infos = self.stock_info_client.get_classified_stocks()
        for _index, code_id in stock_infos['code'].iteritems():
            if self.stock_info_client.is_released(code_id, _date):
                if not self.cdelist_info_client.is_dlisted(code_id, _date):
                    num += 1
                    if self.chaled_info_client.is_halted(code_id, _date):
                        _date = self.calendar_info_client.pre_trading_day(_date) 
                        tmp_data = self.stock_info_client.get_stock_data(code_id, _date, 'D')
                    else:
                        tmp_data = self.stock_info_client.get_stock_data(code_id, _date, dtype)
                    tmp_data['code'] = code_id
                    tmp_data = tmp_data.reset_index(drop = True)
                    total_data = tmp_data if 0 == _index else total_data.append(tmp_data).drop_duplicates()
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

        open_av_price = 0 if num == 0 else open_price/num 
        close_av_price = 0 if num == 0 else close_price/num 
        low_av_price = 0 if num == 0 else low_price/num 
        high_av_price = 0 if num == 0 else high_price/num 
        volume_av_price = 0 if num == 0 else volume/num
        return DataFrame({'code':[self.code],'date':[_date],'open':[open_av_price],'close':[close_av_price],'high':[high_av_price],'volume':[volume_av_price]})  

    #@trace_func(log = logger)
    #def init_average_price(self):
    #    start_date = ct.START_DATE
    #    _today = datetime.now().strftime('%Y-%m-%d')
    #    num_days = delta_days(start_date, _today)
    #    start_date_dmy_format = time.strftime("%d/%m/%Y", time.strptime(start_date, "%Y-%m-%d"))
    #    data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
    #    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())
    #    total_data = None
    #    for _date in date_only_array:
    #        if self.calendar_info_client.is_trading_day(_date):
    #            tmp_data = self.compute(_date)
    #            total_data = tmp_data if total_data is None else total_data.append(tmp_data).drop_duplicates(subset = 'date')
    #            self.mysql_client.set(total_data, self.data_type_dict['D'])

if __name__ == '__main__':
    av = CAverIndex("name","800000",ct.DB_INFO,"stock","combination","calendar", "delisted", "halted")
    av.init()
