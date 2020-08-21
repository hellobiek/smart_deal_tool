#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import pandas as pd
from common import get_tushare_client
if __name__ == '__main__':
    #tu_client = get_tushare_client()
    df = tu_client.trade_cal(exchange='', start_date='20190101', end_date='20201231')
    df = df[['cal_date', 'is_open']]
    df = df.rename(columns = {"cal_date":"calendarDate", "is_open":"isOpen"})
    df['calendarDate'] = df['calendarDate'].astype(str)
    df['calendarDate'] = pd.to_datetime(df.calendarDate).dt.strftime("%Y-%m-%d")
    old_df = pd.read_csv('/conf/calAll.csv', header = 0, encoding = "utf8", names = ['calendarDate', 'isOpen'])
    new_df = old_df.append(df)
    new_df = new_df.drop_duplicates()
    new_df = new_df.reset_index(drop = True)
    new_df.to_csv('/conf/calAll.csv')
