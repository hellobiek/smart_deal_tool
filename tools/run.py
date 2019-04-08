#encoding=utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import datetime
import subprocess
import const as ct
from base.clog import getLogger
from ccalendar import CCalendar
from datetime import datetime
DATA_PROCESS_SCRIPT = '/Users/hellobiek/Documents/workspace/golang/bin/tdx'
class DataPreparer:
    def __init__(self):
        self.logger = getLogger(__name__)
        self.cal_client = CCalendar(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1', filepath = '/Volumes/data/quant/stock/conf/calAll.csv')

    def is_collecting_time(self):
        now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        aft_open_hour,aft_open_minute,aft_open_second = (16,00,00)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (22,00,00)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        #self.logger.info("collecting now time. open_time:%s < now_time:%s < close_time:%s" % (aft_open_time, now_time, aft_close_time))
        return aft_open_time < now_time < aft_close_time

    def prepare_data(self, cmd, timeout = 3600):
        try:
            result = subprocess.run(cmd, timeout).returncode
        except subprocess.TimeoutExpired:
            self.logger.info("run {} timeout".format(cmd))
            result = False
        return result

    def update(self, sleep_time):
        succeed = False
        while True:
            try:
                self.logger.debug("enter update")
                if self.cal_client.is_trading_day(): 
                    if self.is_collecting_time():
                        if not succeed:
                            succeed = True if 0 == self.prepare_data(DATA_PROCESS_SCRIPT) else False
                    else:
                        succeed = False
            except Exception as e:
                self.logger.error(e)
            time.sleep(sleep_time)

dp = DataPreparer()
dp.update(600)
