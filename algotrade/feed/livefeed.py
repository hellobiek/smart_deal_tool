# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(abspath(__file__)))
import bar
import time
import queue
import datetime
import threading
import dataFramefeed
import const as ct
from pyalgotrade.utils import dt
from pyalgotrade.dataseries import DEFAULT_MAX_LEN
from subscriber import Subscriber, StockQuoteHandler, OrderBookHandler
from log import getLogger
logger = getLogger(__name__)

def localnow():
    return dt.as_utc(datetime.datetime.now())

class PollingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.__stopped = True

    def __wait(self):
        next_call_time = self.getNextCallDateTime()
        start_time = localnow()
        while not self.__stopped and localnow() < next_call_time:
            time.sleep((next_call_time - start_time).seconds)

    def stopped(self):
        return self.__stopped

    def run(self):
        while not self.__stopped:
            self.__wait()
            if not self.__stopped:
                try:
                    self.doCall()
                except Exception as e:
                    logger.error("unhandled exception:%s" % e)

    def start(self):
        self.__stopped = False
        threading.Thread.start(self)

    def stop(self):
        self.__stopped = True

    def getNextCallDateTime(self):
        raise NotImplementedError()

    def doCall(self):
        raise NotImplementedError()

def build_bar(quote_dict, order_dict):
    time_str = "%s %s" % (quote_dict["date"][0], quote_dict["time"][0])
    sdatetime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    return bar.BasicTick(sdatetime, quote_dict['open'][0], quote_dict['high'][0], quote_dict['low'][0], quote_dict['close'][0], quote_dict['preclose'][0], quote_dict['volume'][0], quote_dict['amount'][0],
                        order_dict['Bid'][0][0], order_dict['Bid'][0][1], order_dict['Bid'][1][0], order_dict['Bid'][1][1], order_dict['Bid'][2][0], order_dict['Bid'][2][1], order_dict['Bid'][3][0], order_dict['Bid'][3][1], order_dict['Bid'][4][0], order_dict['Bid'][4][1],
                        order_dict['Ask'][0][0], order_dict['Ask'][0][1], order_dict['Ask'][1][0], order_dict['Ask'][1][1], order_dict['Ask'][2][0], order_dict['Ask'][2][1], order_dict['Ask'][3][0], order_dict['Ask'][3][1], order_dict['Ask'][4][0], order_dict['Ask'][4][1],
                        bar.Frequency.TRADE)

class GetBarThread(PollingThread):
    ON_BARS = 1
    ON_END  = 2
    def __init__(self, mqueue, identifiers, frequency = 3):
        PollingThread.__init__(self)
        self.__queue = mqueue
        self.__identifiers = identifiers 
        self.__nextCallDatetime = localnow()
        self.__frequency = frequency
        self.__subscriber = Subscriber()
        self.__last_response_time = None
        self.__end_time = "15:00:00" 

    def getNextCallDateTime(self):
        self.__nextCallDatetime = max(localnow(), self.__nextCallDatetime + self.__frequency)
        return self.__nextCallDatetime

    def doCall(self):
        bar_dict = {}
        for identifier in self.__identifiers:
            quote_ret, quote_data = self.__subscriber.get_quote_data(identifier)
            order_ret, order_data = self.__subscriber.get_order_book_data(identifier)
            if 0 == order_ret and 0 == quote_ret:
                quote_data = quote_data[['data_date', 'data_time', 'open_price', 'high_price', 'low_price', 'last_price', 'prev_close_price', 'volume', 'turnover']]
                quote_data = quote_data.rename(columns = {"data_date": "date", "data_time": "time", "open_price": "open", "high_price": "high", "low_price": "low", "last_price": "close", "prev_close_price": "preclose", "turnover": "amount"})
                bar_dict[identifier] = build_bar(quote_data.to_dict(), order_data)
                self.__last_response_time = quote_data.iloc[-1]['time']
        if len(bar_dict) > 0:
            bars = bar.Ticks(bar_dict)
            if self.__last_response_time == "15:00:00":
                self.__queue.put((GetBarThread.ON_END, bars))
            else:
                self.__queue.put((GetBarThread.ON_BARS, bars))

    def init_real_trading(self):
        for identifier in self.__identifiers:
            quote_ret = self.__subscriber.subscribe_quote(identifier, StockQuoteHandler)
            order_ret = self.__subscriber.subscribe_order_book(identifier, OrderBookHandler)
            if 0 != order_ret or 0 != quote_ret: return False
        return True

    def start(self):
        if not self.__subscriber.status(): self.__subscriber.start(host = ct.FUTU_HOST_LOCAL)
        if not self.init_real_trading(): raise Exception("init_real_trading subscribe failed")
        PollingThread.start(self)

    def stop(self):
        PollingThread.stop(self)
        self.__subscriber.stop()
        self.__subscriber.close()

class LiveFeed(dataFramefeed.TickFeed):
    """
        a real-time BarFeed that builds bars using futu api
        :param identifiers: codes
        :param frequency 每隔几秒钟请求一次，默认3秒钟
        :param maxLen:
    """
    QUEUE_TIMEOUT = 0.01
    def __init__(self, identifiers, frequency = 3, maxLen = DEFAULT_MAX_LEN):
        dataFramefeed.TickFeed.__init__(self, bar.Frequency.TRADE, None, maxLen)
        if not isinstance(identifiers, list): raise Exception("identifiers must be a list")
        self.__queue = queue.Queue()
        self.__thread = GetBarThread(self.__queue, identifiers, datetime.timedelta(seconds = frequency))
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
        return self.__thread.stopped()

    def peekDateTime(self):
        return dataFramefeed.TickFeed.peekDateTime(self)

    def getCurrentDateTime(self):
        return localnow()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        try:
            eventType, eventData = self.__queue.get(True, LiveFeed.QUEUE_TIMEOUT)
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
