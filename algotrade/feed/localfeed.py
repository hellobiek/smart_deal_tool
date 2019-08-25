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
        self.__queue       = mqueue
        self.__start_time  = get_today_time(start_time)
        self.__end_time    = get_today_time(end_time)
        self.__timezone    = timezone
        self.__rstock      = RIndexStock(dbinfo = DBINFO, redis_host = REDIS_HOST)
        self.__calendar    = CCalendar(dbinfo = DBINFO, redis_host = REDIS_HOST, filepath = CAL_FILE_PATH)
        self.__frequency   = frequency
        self.__identifiers = identifiers
        self.__next_call_time = localnow(self.__timezone)

    def getNextCallDateTime(self):
        self.__next_call_time = max(localnow(self.__timezone), self.__next_call_time + self.__frequency)
        return self.__next_call_time

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
        return bar.BasicBar(dateTime, open_, high, low, close, volume, adjClose, self.__frequency, extra = key_dict)

    def doCall(self):
        bar_dict = {}
        mdate = datetime.now().strftime('%Y-%m-%d')
        pre_date = self.__calendar.pre_trading_day(mdate)
        for identifier in self.__identifiers:
            data = CStock(identifier).get_k_data()
            data = kdj(data)
            data = data.loc[data.date == pre_date]
            res = self.parseBar(data)
            if res is not None: bar_dict[identifier] = res
        if len(bar_dict) > 0:
            bars = bar.Bars(bar_dict)
            self.__queue.put((ON_BARS, bars))

    def updateIdentifiers(self, identifiers):
        self.__identifiers = identifiers

    def wait(self):
        next_call_time = self.getNextCallDateTime()
        begin_time = localnow(self.__timezone)
        while not self.stopped and localnow(self.__timezone) < next_call_time:
            time.sleep((next_call_time - begin_time).seconds)

    def start(self):
        PollingThread.start(self)

    def stop(self):
        PollingThread.stop(self)

    def run(self):
        while not self.stopped:
            self.wait()
            if not self.stopped:
                try:
                    self.doCall()
                except Exception as e:
                    print("unhandled exception:{}".format(e))

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
        self.__timezone = timezone
        self.__queue = queue.Queue()
        self.__calendar = CCalendar(dbinfo = DBINFO, redis_host = REDIS_HOST, filepath = CAL_FILE_PATH)
        self.__broker = broker
        self.__start_time = dealtime['start']
        self.__end_time   = dealtime['end']
        self.__selector = model
        self.__thread = GetBarThread(self.__queue, identifiers, self.__start_time, self.__end_time, timedelta(seconds = frequency), self.__timezone)
        for instrument in identifiers: self.registerInstrument(instrument)

    def start(self):
        if self.__thread.is_alive(): raise Exception("already strated")
        self.__thread.start()

    def stop(self):
        self.__thread.stop()

    def join(self):
        if self.__thread.is_alive():
            self.__thread.join()

    def eof(self):
        return self.__thread.stopped

    def peekDateTime(self):
        return dataFramefeed.Feed.peekDateTime(self)

    def updateIdentifiers(self, mdate):
        self.reset()
        df = self.__selector.get_data(mdate)
        positions = self.__broker.getPositions()
        if df.empty and len(positions) == 0: return None
        identifiers = df.code.tolist()
        hold_codes = [code.split('.')[1] for code in list(positions.keys())]
        identifiers.extend(hold_codes)
        for instrument in identifiers: self.registerInstrument(instrument)
        self.__thread.updateIdentifiers(identifiers)

    def getCurrentDateTime(self):
        return localnow(self.__timezone)

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        try:
            mdate = datetime.now().strftime('%Y-%m-%d')
            pre_date = self.__calendar.pre_trading_day(mdate)
            self.updateIdentifiers(pre_date)
            eventType, eventData = self.__queue.get(True, LocalFeed.QUEUE_TIMEOUT)
            if eventType == ON_BARS:
                ret = eventData
            elif eventType == ON_END:
                ret = eventData
                self.stop()
            else:
                self.logger.error("invalid event received:{}-{}".format(eventType, eventData))
        except queue.Empty:
            self.logger.debug("get empty queue")
        return ret
