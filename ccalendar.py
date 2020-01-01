#coding=utf-8
import cmysql
import _pickle
import const as ct
import pandas as pd
from datetime import datetime
from base.cdate import get_dates_array
from common import create_redis_obj, df_delta
class CCalendar(object):
    data = None
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None, without_init = True, filepath = ct.CALENDAR_PATH):
        self.fpath = filepath 
        self.table = ct.CALENDAR_TABLE
        self.trigger = ct.SYNCCAL2REDIS
        self.redis = create_redis_obj(host = 'redis-proxy-container', port = 6579) if redis_host is None else create_redis_obj(host = redis_host, port = 6579)
        if not without_init:
            if not self.init(): raise Exception("init ccalendar failed")
        CCalendar.data = self.get_data()

    def init(self):
        df = pd.read_csv(self.fpath)
        CCalendar.data = df
        return self.redis.set(ct.CALENDAR_INFO, _pickle.dumps(df, 2))

    def get_data(self):
        df_byte = self.redis.get(ct.CALENDAR_INFO)
        if df_byte is None:
            raise Exception("calendar data in redis is None")
        df = _pickle.loads(df_byte)
        return df

    def create(self):
        sql = 'create table if not exists %s(calendarDate varchar(10) not null, isOpen int, PRIMARY KEY (calendarDate))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('calendarDate', NEW.calendarDate, 'isOpen', NEW.isOpen));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    def is_trading_day(self, mdate = None):
        tmp_date = mdate if mdate is not None else datetime.now().strftime('%Y-%m-%d')
        return 1 == CCalendar.data.loc[CCalendar.data.calendarDate == tmp_date].isOpen.values[0]

    def trading_day_series(self, start, end):
        date_array = get_dates_array(start, end, asending = True)
        return [mdate for mdate in date_array if self.is_trading_day(mdate)]

    def get(self, mdate = None):
        return CCalendar.data if mdate is None else CCalendar.data.loc[CCalendar.data.calendarDate == mdate]

    def pre_trading_day(self, mdate):
        index = CCalendar.data[CCalendar.data.calendarDate == mdate].index.tolist()[0]
        if index > 0:
            tindex = index
            while tindex > 0:
                tindex -= 1
                if CCalendar.data.isOpen[tindex] == 1: return CCalendar.data.calendarDate[tindex]
        return None

if __name__ == '__main__':
    ccalendar = CCalendar(ct.DB_INFO, without_init = False)
    mdate = '2020-01-01'
    ccalendar.is_trading_day(mdate)
