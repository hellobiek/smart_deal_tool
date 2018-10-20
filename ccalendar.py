#coding=utf-8
import cmysql
import _pickle
import const as ct
import pandas as pd
from log import getLogger
from datetime import datetime
from common import trace_func, create_redis_obj, df_delta
logger = getLogger(__name__)
class CCalendar:
    def __init__(self, dbinfo = ct.DB_INFO, without_init = False, redis_host = None):
        self.table = ct.CALENDAR_TABLE
        self.trigger = ct.SYNCCAL2REDIS
        self.redis = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.mysql_client = cmysql.CMySQL(dbinfo, iredis = self.redis)
        if without_init == False:
            if not self.create(): raise Exception("create calendar table failed")
            if not self.init(True): raise Exception("calendar table init failed")
            # here must be first init and second register, for init will delete table
            # which will delete trigger
            if not self.register(): raise Exception("create calendar trigger failed") 
  
    def create(self):
        sql = 'create table if not exists %s(calendarDate varchar(10) not null, isOpen int, PRIMARY KEY (calendarDate))' % self.table
        return True if self.table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, self.table)

    def register(self):
        sql = "create trigger %s after insert on %s for each row set @set=gman_do_background('%s', json_object('calendarDate', NEW.calendarDate, 'isOpen', NEW.isOpen));" % (self.trigger, self.table, self.trigger)
        return True if self.trigger in self.mysql_client.get_all_triggers() else self.mysql_client.register(sql, self.trigger)

    def init(self, status):
        old_trading_day = self.get()
        new_trading_day = pd.read_csv('/conf/calAll.csv')
        if new_trading_day is None: return False
        if new_trading_day.empty: return False
        if not old_trading_day.empty:
            new_trading_day = df_delta(new_trading_day, old_trading_day, ['calendarDate'])
        if new_trading_day.empty: return True
        res = self.mysql_client.set(new_trading_day, self.table)
        if not res: return False
        if status: return self.redis.set(ct.CALENDAR_INFO, _pickle.dumps(new_trading_day, 2))

    @staticmethod
    def is_trading_day(_date = None, redis = None):
        _redis = create_redis_obj() if redis is None else redis
        df = CCalendar.get(redis = _redis)
        tmp_date = _date if _date is not None else datetime.now().strftime('%Y-%m-%d')
        return True if df.empty else 1 == df.loc[df.calendarDate == tmp_date].isOpen.values[0]

    @staticmethod
    def get(_date = None, redis = None):
        _redis = create_redis_obj() if redis is None else redis
        df_byte = _redis.get(ct.CALENDAR_INFO)
        if df_byte is None: return pd.DataFrame() 
        df = _pickle.loads(df_byte)
        return df if _date is None else df.loc[df.calendarDate == _date]

    def pre_trading_day(self, _date):
        df = self.get()
        _index = df[df.calendarDate == _date].index.tolist()[0]
        if _index > 0:
            _tindex = _index
            while _tindex > 0:
                _tindex -= 1
                if df.isOpen[_tindex] == 1: return df.calendarDate[_tindex]
        return None

if __name__ == '__main__':
    ccalendar = CCalendar(ct.DB_INFO, "calendar")
    ccalendar.init(False)
