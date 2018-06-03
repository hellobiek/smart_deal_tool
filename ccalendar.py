#coding=utf-8
import cmysql
import const as ct
import pandas as pd
import tushare as ts
from log import getLogger
from datetime import datetime
from common import trace_func

logger = getLogger(__name__)

class CCalendar:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        self.table = table_name
        self.mysql_client = cmysql.CMySQL(dbinfo)
        if not self.create(): raise Exception("create calendar table failed")
   
    @trace_func(log = logger)
    def create(self):
        sql = 'create table if not exists `%s`(calendarDate varchar(10),isOpen int)' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql)

    @trace_func(log = logger)
    def init(self):
        old_trading_day = self.mysql_client.get(ct.SQL % self.table) 
        new_trading_day = ts.trade_cal()
        if new_trading_day is not None:
            trading_day = pd.merge(old_trading_day,new_trading_day,how='outer')
            self.mysql_client.set(trading_day, self.table)

    @trace_func(log = logger)
    def is_trading_day(self, _date = None):
        tmp_date = _date if _date is not None else datetime.now().strftime('%Y-%m-%d')  
        stock_dates_df = self.mysql_client.get(ct.SQL % self.table)
        return True if stock_dates_df.empty else 1 == stock_dates_df.query('calendarDate=="%s"' % tmp_date).isOpen.values[0]

    @trace_func(log = logger)
    def get(self, _date = None):
        if _date is None:
            sql = "select * from %s" % self.table
        else:
            sql = "select * from %s where date = %s" % (self.table, _date)
        return self.mysql_client.get(sql)

    @trace_func(log = logger)
    def pre_trading_day(self, _date):
        df = self.get()
        _index = df[df.calendarDate == _date].index.tolist()[0]
        if _index > 0:
            _tindex = _index
            while _tindex > 0:
                _tindex -= 1
                if df['isOpen'][_tindex] == 1: return df['calendarDate'][_tindex]
        return None

if __name__ == '__main__':
    ccalendar = CCalendar(ct.DB_INFO, "calendar")
    ccalendar.is_trading_day('2018-05-18')
    ccalendar.is_trading_day('2018-05-20')
