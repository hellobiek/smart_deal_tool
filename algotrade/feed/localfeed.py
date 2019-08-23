# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(abspath(__file__)))
import bar
import time
import queue
import const as ct
import dataFramefeed
from base.clog import getLogger
from datetime import timedelta, datetime
from pyalgotrade.dataseries import DEFAULT_MAX_LEN
from base.base import PollingThread, localnow, get_today_time
ON_END  = 1
ON_BARS = 2
class GetBarThread(PollingThread):
    def __init__(self, mqueue, identifiers, start_time, end_time, frequency, timezone):
        PollingThread.__init__(self)
        self.__queue       = mqueue
        self.__start_time  = get_today_time(start_time)
        self.__end_time    = get_today_time(end_time)
        self.__timezone    = timezone
        self.__frequency   = frequency
        self.__identifiers = identifiers 
        self.__next_call_time = localnow(self.__timezone)
        self.__last_response_time = localnow(self.__timezone)

    def getNextCallDateTime(self):
        self.__next_call_time = max(localnow(self.__timezone), self.__next_call_time + self.__frequency)
        return self.__next_call_time

    def build_bar(self):
        if self.__last_response_time >= sdatetime: return None
        self.__last_response_time = sdatetime
        return bar.BasicTick(bar.Frequency.TRADE)

    def doCall(self):
        bar_dict = {}
        for identifier in self.__identifiers:
            res = self.build_bar()
            if res is not None: bar_dict[identifier] = res
        if len(bar_dict) > 0:
            bars = bar.Ticks(bar_dict)
            self.__queue.put((ON_BARS, bars))
        if localnow(self.__timezone) >= self.__end_time:
            self.stop()

    def init_instruments(self):
        return True

    def wait(self):
        next_call_time = self.getNextCallDateTime()
        begin_time = localnow(self.__timezone)
        while not self.stopped and localnow(self.__timezone) < next_call_time:
            time.sleep((next_call_time - begin_time).seconds)

    def start(self):
        if not self.init_instruments(): raise Exception("instruments subscribe failed")
        PollingThread.start(self)

    def run(self):
        while not self.stopped:
            self.wait()
            if not self.stopped:
                try:
                    self.doCall()
                except Exception as e:
                    print("unhandled exception:%s" % e)

    def stop(self):
        PollingThread.stop(self)

class LocalFeed(dataFramefeed.TickFeed):
    """
        a real-time BarFeed that builds bars using futu api
        :param identifiers: codes
        :param frequency 每隔几秒钟请求一次，默认3秒钟
        :param maxLen:
    """
    QUEUE_TIMEOUT = 0.01
    def __init__(self, identifiers, timezone, dealtime, frequency = 24 * 60 * 60, maxLen = DEFAULT_MAX_LEN):
        dataFramefeed.TickFeed.__init__(self, bar.Frequency.TRADE, None, maxLen)
        if not isinstance(identifiers, list): raise Exception("identifiers must be a list")
        self.__timezone = timezone
        self.__queue  = queue.Queue()
        self.__start_time = dealtime['start']
        self.__end_time   = dealtime['end']
        self.logger = getLogger(__name__)
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
        return dataFramefeed.TickFeed.peekDateTime(self)

    def getCurrentDateTime(self):
        return localnow(self.__timezone)

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        try:
            eventType, eventData = self.__queue.get(True, FutuFeed.QUEUE_TIMEOUT)
            if eventType == ON_BARS:
                ret = eventData
            elif eventType == ON_END:
                ret = eventData
                self.stop()
            else:
                self.logger.error("invalid event received: {} - {}".format(eventType, eventData))
        except queue.Empty:
            self.logger.info("get empty queue")
        return ret
