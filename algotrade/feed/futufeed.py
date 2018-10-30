# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(abspath(__file__)))
import bar
import time
import queue
import threading
import const as ct
import dataFramefeed
from datetime import timedelta, datetime
from base.base import PollingThread, localnow, get_today_time
from pyalgotrade.dataseries import DEFAULT_MAX_LEN
from subscriber import Subscriber, StockQuoteHandler, OrderBookHandler
from log import getLogger
logger = getLogger(__name__)
class GetBarThread(PollingThread):
    ON_END  = 1
    ON_BARS = 2
    def __init__(self, mqueue, identifiers, start_time, end_time, frequency, timezone):
        PollingThread.__init__(self)
        self.__queue       = mqueue
        self.__start_time  = get_today_time(start_time)
        self.__end_time    = get_today_time(end_time)
        self.__timezone    = timezone
        self.__frequency   = frequency
        self.__identifiers = identifiers 
        self.__subscriber  = Subscriber()
        self.__next_call_time = localnow(self.__timezone)
        self.__last_response_time = localnow(self.__timezone)

    def getNextCallDateTime(self):
        self.__next_call_time = max(localnow(self.__timezone), self.__next_call_time + self.__frequency)
        return self.__next_call_time

    def build_bar(self, quote_dict, order_dict):
        time_str = "%s %s" % (quote_dict["date"][0], quote_dict["time"][0])
        sdatetime = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        if self.__last_response_time >= sdatetime: return None
        self.__last_response_time = sdatetime
        return bar.BasicTick(sdatetime, quote_dict['open'][0], quote_dict['high'][0], quote_dict['low'][0], quote_dict['close'][0], quote_dict['preclose'][0], quote_dict['volume'][0], quote_dict['amount'][0],
                            order_dict['Bid'][0][0], order_dict['Bid'][0][1], order_dict['Bid'][1][0], order_dict['Bid'][1][1], order_dict['Bid'][2][0], order_dict['Bid'][2][1], order_dict['Bid'][3][0], order_dict['Bid'][3][1], order_dict['Bid'][4][0], order_dict['Bid'][4][1],
                            order_dict['Ask'][0][0], order_dict['Ask'][0][1], order_dict['Ask'][1][0], order_dict['Ask'][1][1], order_dict['Ask'][2][0], order_dict['Ask'][2][1], order_dict['Ask'][3][0], order_dict['Ask'][3][1], order_dict['Ask'][4][0], order_dict['Ask'][4][1],
                            bar.Frequency.TRADE)

    def doCall(self):
        bar_dict = {}
        for identifier in self.__identifiers:
            quote_ret, quote_data = self.__subscriber.get_quote_data(identifier)
            order_ret, order_data = self.__subscriber.get_order_book_data(identifier)
            if 0 == order_ret and 0 == quote_ret:
                quote_data = quote_data[['data_date', 'data_time', 'open_price', 'high_price', 'low_price', 'last_price', 'prev_close_price', 'volume', 'turnover']]
                quote_data = quote_data.rename(columns = {"data_date": "date", "data_time": "time", "open_price": "open", "high_price": "high", "low_price": "low", "last_price": "close", "prev_close_price": "preclose", "turnover": "amount"})
                res = self.build_bar(quote_data.to_dict(), order_data)
                if res is not None: bar_dict[identifier] = res
        if len(bar_dict) > 0:
            bars = bar.Ticks(bar_dict)
            self.__queue.put((GetBarThread.ON_BARS, bars))
        if localnow(self.__timezone) >= self.__end_time:
            self.stop()

    def init_real_trading(self):
        for identifier in self.__identifiers:
            quote_ret = self.__subscriber.subscribe_quote(identifier, StockQuoteHandler)
            order_ret = self.__subscriber.subscribe_order_book(identifier, OrderBookHandler)
            if 0 != order_ret or 0 != quote_ret: return False
        return True

    def wait(self):
        next_call_time = self.getNextCallDateTime()
        begin_time = localnow(self.__timezone)
        while not self.stopped and localnow(self.__timezone) < next_call_time:
            time.sleep((next_call_time - begin_time).seconds)

    def start(self):
        if not self.__subscriber.status(): self.__subscriber.start(host = ct.FUTU_HOST_LOCAL)
        if not self.init_real_trading(): raise Exception("init_real_trading subscribe failed")
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
        self.__subscriber.stop()
        self.__subscriber.close()

class FutuFeed(dataFramefeed.TickFeed):
    """
        a real-time BarFeed that builds bars using futu api
        :param identifiers: codes
        :param frequency 每隔几秒钟请求一次，默认3秒钟
        :param maxLen:
    """
    QUEUE_TIMEOUT = 0.01
    def __init__(self, identifiers, timezone, dealtime, frequency = 3, maxLen = DEFAULT_MAX_LEN):
        dataFramefeed.TickFeed.__init__(self, bar.Frequency.TRADE, None, maxLen)
        if not isinstance(identifiers, list): raise Exception("identifiers must be a list")
        self.__timezone = timezone
        self.__start_time = dealtime['start']
        self.__end_time   = dealtime['end']
        self.__queue  = queue.Queue()
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
            if eventType == GetBarThread.ON_BARS:
                ret = eventData
            elif eventType == GetBarThread.ON_END:
                ret = eventData
                self.stop()
            else:
                logger.error("Invalid event received: %s - %s" % (eventType, eventData))
        except queue.Empty:
            pass
        return ret
