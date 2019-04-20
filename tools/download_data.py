#encoding=utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import datetime
import subprocess
import const as ct
from threading import Timer
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from common import get_latest_data_date, transfer_date_string_to_int
SCRIPT1 = 'python3 /Users/hellobiek/Documents/workspace/python/quant/DTGear/cli.py update report'
SCRIPT2 = '/Users/hellobiek/Documents/workspace/golang/bin/tdx'
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

    def prepare_data(self, cmds, timeout = 2700):
        kill = lambda process: process.kill()
        cmd_list = list()
        for cmd in cmds: cmd_list.append(subprocess.Popen(cmd, shell=True))
        my_timer = Timer(timeout, kill, cmd_list)
        try:
            my_timer.start()
            for cmd in cmd_list: cmd.communicate()
        finally:
            my_timer.cancel()

    def update(self, sleep_time):
        while True:
            try:
                self.logger.debug("enter update")
                if self.cal_client.is_trading_day(): 
                    if self.is_collecting_time():
                        ndate = get_latest_data_date(filepath = "/Volumes/data/quant/stock/data/stockdatainfo.json")
                        mdate = transfer_date_string_to_int(datetime.now().strftime('%Y-%m-%d'))
                        if ndate < mdate: self.prepare_data([SCRIPT1, SCRIPT2])
            except Exception as e:
                self.logger.error(e)
            time.sleep(sleep_time)

dp = DataPreparer()
dp.update(3000)
