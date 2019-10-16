# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(abspath(__file__)))
import bar
import time
import queue
import numpy as np
import const as ct
import dataFramefeed
from cstock import CStock
from rstock import RIndexStock
from base.clog import getLogger
from ccalendar import CCalendar
from base.cdate import parse_date
from algotrade.technical.kdj import kdj
from datetime import datetime, timedelta
from pyalgotrade.dataseries import DEFAULT_MAX_LEN
from base.base import PollingThread, localnow, get_today_time
ON_END  = 1
ON_BARS = 2
DBINFO = ct.OUT_DB_INFO
REDIS_HOST = "127.0.0.1"
CAL_FILE_PATH = "/Volumes/data/quant/stock/conf/calAll.csv"
class GetBarThread(PollingThread):
    def __init__(self, mqueue, identifiers, start_time, end_time, frequency, timezone):
        PollingThread.__init__(self)
        self.logger = getLogger(__name__)
        self.queue = mqueue
        self.start_time = get_today_time(start_time)
        self.end_time = get_today_time(end_time)
        self.timezone = timezone
        self.calendar = CCalendar(dbinfo = DBINFO, redis_host = REDIS_HOST, filepath = CAL_FILE_PATH)
        self.frequency = frequency
        self.identifiers = identifiers
        self.next_call_time = get_today_time(start_time)

    def getNextCallDateTime(self):
        now_time = localnow(self.timezone)
        start_time = datetime(now_time.year, now_time.month, now_time.day,self.start_time.hour, self.start_time.minute, self.start_time.second)
        if now_time < start_time:
            self.next_call_time = start_time
        else:
            self.next_call_time = start_time + self.frequency
        return self.next_call_time

    def parseBar(self, data):
        row = data.to_dict("records")[0]
        dateTime = parse_date(row['date'])
        open_ = float(row['open'])
        high = float(row['high'])
        low = float(row['low'])
        close = float(row['close'])
        volume = float(row['volume'])
        adjClose = float(row['close'])
        key_dict = dict()
        origin_keys = row.keys()
        normal_keys = ['date', 'open', 'high', 'low', 'close', 'volume', 'code']
        special_keys = list(set(origin_keys).difference(set(normal_keys)))
        for sitem in special_keys:
            value = None if np.isnan(row[sitem]) else float(row[sitem])
            key_dict[sitem] = value
        return bar.BasicBar(dateTime, open_, high, low, close, volume, adjClose, self.frequency, extra = key_dict)

    def doCall(self):
        bar_dict = {}
        mdate = datetime.now().strftime('%Y-%m-%d')
        pre_date = self.calendar.pre_trading_day(mdate)
        for identifier in self.identifiers:
            data = CStock(identifier).get_k_data()
            data = kdj(data)
            data = data.loc[data.date == pre_date]
            res = self.parseBar(data)
            if res is not None: bar_dict[identifier] = res
        if len(bar_dict) > 0:
            bars = bar.Bars(bar_dict)
            self.queue.put((ON_BARS, bars))

    def updateIdentifiers(self, identifiers):
        self.identifiers = identifiers

    def wait(self):
        now_time = localnow(self.timezone)
        next_call_time = self.getNextCallDateTime()
        self.logger.info("beigin time:{}, next call time:{}".format(now_time, next_call_time))
        if not self.stopped and now_time < next_call_time:
            sleep_times = (next_call_time - now_time).total_seconds()
            self.logger.info("sleep time:{}".format(sleep_times))
            time.sleep(sleep_times)

    def start(self):
        PollingThread.start(self)

    def stop(self):
        PollingThread.stop(self)

    def run(self):
        while not self.stopped:
            self.wait()
            if not self.stopped and self.calendar.is_trading_day():
                try:
                    self.doCall()
                except Exception as e:
                    self.logger.error("unhandled exception:{}".format(e))
            #self.wait()

class LocalFeed(dataFramefeed.Feed):
    """
        a real-time BarFeed that builds bars using futu api
        :param identifiers: codes
        :param frequency 每隔几秒钟请求一次，默认3秒钟
        :param maxLen:
    """
    QUEUE_TIMEOUT = 0.01
    def __init__(self, model, broker, identifiers, timezone, dealtime, frequency, maxLen = DEFAULT_MAX_LEN):
        dataFramefeed.Feed.__init__(self, bar.Frequency.DAY, None, maxLen)
        if not isinstance(identifiers, list): raise Exception("identifiers must be a list")
        self.logger = getLogger(__name__)
        self.timezone = timezone
        self.queue = queue.Queue()
        self.calendar = CCalendar(dbinfo = DBINFO, redis_host = REDIS_HOST, filepath = CAL_FILE_PATH)
        self.broker = broker
        self.start_time = dealtime['start']
        self.end_time   = dealtime['end']
        self.selector = model
        self.thread = GetBarThread(self.queue, identifiers, self.start_time, self.end_time, timedelta(seconds = frequency), self.timezone)
        for instrument in identifiers: self.registerInstrument(instrument)

    def start(self):
        if self.thread.is_alive(): raise Exception("already strated")
        self.updateIdentifiers()
        self.thread.start()

    def stop(self):
        self.thread.stop()

    def join(self):
        if self.thread.is_alive():
            self.thread.join()

    def eof(self):
        return self.thread.stopped

    def peekDateTime(self):
        return dataFramefeed.Feed.peekDateTime(self)

    def updateIdentifiers(self):
        self.reset()
        mdate = datetime.now().strftime('%Y-%m-%d')
        pre_date = self.calendar.pre_trading_day(mdate)
        df = self.selector.get_stock_pool(pre_date)
        positions = self.broker.getPositions()
        if df.empty and len(positions) == 0: return None
        identifiers = df.code.tolist() if not df.empty else list()
        hold_codes = [code.split('.')[1] for code in list(positions.code.tolist())]
        identifiers.extend(hold_codes)
        for instrument in identifiers: self.registerInstrument(instrument)
        self.thread.updateIdentifiers(identifiers)

    def getCurrentDateTime(self):
        return localnow(self.timezone)

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        try:
            time.sleep(60)
            self.updateIdentifiers()
            eventType, eventData = self.queue.get(True, LocalFeed.QUEUE_TIMEOUT)
            if eventType == ON_BARS:
                ret = eventData
            elif eventType == ON_END:
                ret = eventData
                self.stop()
            else:
                self.logger.error("invalid event received:{}-{}".format(eventType, eventData))
            return ret
        except queue.Empty:
            pass
        except Exception as e:
            self.logger.error("exception is {}".format(e))
