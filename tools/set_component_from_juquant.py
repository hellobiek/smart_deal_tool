# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from cindex import CIndex
from jqdatasdk import auth
from ccalendar import CCalendar
from cstock_info import CStockInfo
from base.cdate import get_day_nday_ago, get_dates_array
if __name__ == '__main__':
    num = 5500
    end_date = '2019-08-13'
    stock_info_client = CStockInfo()
    df = stock_info_client.get()
    code_list = df['code'].tolist()
    name_list = df['name'].tolist()
    code2namedict = dict(zip(code_list, name_list))
    start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
    date_array = get_dates_array(start_date, end_date)
    auth('18701683341', '52448oo78')
    for code in ['000001', '000016', '000300', '000905', '399001', '399005', '399673']:
        obj = CIndex(code)
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate):
                table_name = obj.get_components_table_name(mdate)
                if obj.is_table_exists(table_name): obj.mysql_client.delete(table_name)
            
        for mdate in date_array:
            if CCalendar.is_trading_day(mdate):
                if not obj.set_components_data_from_joinquant(code2namedict, mdate):
                    print("{} for {} set failed".format(code, mdate))
