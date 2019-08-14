# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import time
import datetime
import subprocess
import const as ct
from datetime import datetime
from ccalendar import CCalendar
from base.clog import getLogger
from base.cdate import transfer_date_string_to_int
from common import get_latest_data_date
SCRIPT1 = ['python3', '-B', '/Users/hellobiek/Documents/workspace/python/quant/DTGear/cli.py', 'update', 'report']
SCRIPT2 = ['/Users/hellobiek/Documents/workspace/golang/bin/tdx']
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

    def run(self, cmd, timeout):
        self.logger.info("start to run cmd:%s, timeout:%s" % (cmd, timeout))
        proc = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        finished = False
        for t in range(timeout):
            if not finished:
                time.sleep(2)
                if proc.poll() is not None:
                    outs, errs = proc.communicate()
                    self.logger.debug("stdout:%s, stderr:%s" % (outs.decode("utf-8"), errs.decode("utf-8")))
                    finished = True
                else:
                    for stdout_line in proc.stdout:
                        self.logger.debug("stdout:%s" % stdout_line.decode("utf-8"))
            else:
                if proc.poll() is None:
                    self.logger.error("kill process after finished, cmd:%s, pid:%s" % (cmd, proc.pid))
                    proc.kill()
                return
        self.logger.error("kill process for not finished, cmd:%s, pid:%s" % (cmd, proc.pid))
        proc.kill()

    def update(self, sleep_time):
        while True:
            try:
                self.logger.debug("enter update")
                if self.cal_client.is_trading_day(redis = self.cal_client.redis):
                    if self.is_collecting_time():
                        ndate = get_latest_data_date(filepath = "/Volumes/data/quant/stock/data/stockdatainfo.json")
                        mdate = transfer_date_string_to_int(datetime.now().strftime('%Y-%m-%d'))
                        if ndate < mdate:
                            self.run(SCRIPT1, timeout = 600)
                            self.run(SCRIPT2, timeout = 2400)
            except Exception as e:
                self.logger.error(e)
            time.sleep(sleep_time)

dp = DataPreparer()
dp.update(3500)
