#-*-coding:utf-8-*-
import queue
import threading
import const as ct
from log import getLogger
from gevent.lock import Semaphore
from futuquant import OpenQuoteContext
from futuquant.quote.quote_response_handler import OrderBookHandlerBase, TickerHandlerBase, StockQuoteHandlerBase
logger = getLogger(__name__)
class StockQuoteHandler(StockQuoteHandlerBase):
    def __init__(self):
        super(StockQuoteHandler, self).__init__()
        self.__lock  = Semaphore(1)
        self.__queue = queue.Queue()

    def empty(self):
        with self.__lock:
            return self.__queue.empty()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_str):
        ret, data = super(StockQuoteHandler, self).on_recv_rsp(rsp_str)
        if ret == 0: self.__queue.put(data)

class TickerHandler(TickerHandlerBase):
    def __init__(self):
        super(TickerHandler, self).__init__()
        self.__lock  = Semaphore(1)
        self.__queue = queue.Queue()

    def empty(self):
        with self.__lock:
            return self.__queue.empty()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_pb):
        ret, data = super(TickerHandler, self).on_recv_rsp(rsp_pb)
        if ret == 0: self.__queue.put(data)

class OrderBookHandler(OrderBookHandlerBase):
    def __init__(self):
        super(OrderBookHandler, self).__init__()
        self.__lock  = Semaphore(1)
        self.__queue = queue.Queue()

    def empty(self):
        with self.__lock:
            return self.__queue.empty()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_pb):
        ret, data = super(OrderBookHandler, self).on_recv_rsp(rsp_pb)
        if ret == 0: self.__queue.put(data)

class Subscriber:
    def __init__(self):
        self._status = False
        self.sub_dict = None
        self.quote_ctx = None
        self.lock = Semaphore(1)

    def __del__(self):
        if self.quote_ctx is not None:
            self.quote_ctx.close()

    def start(self, host = ct.FUTU_HOST, port = ct.FUTU_PORT):
        with self.lock:
            if self.quote_ctx is None:
                self.quote_ctx = OpenQuoteContext(host, port)
            else:
                self.quote_ctx.start()
            self.sub_dict = self.get_subscribed_dict()
            logger.debug("self.sub_dict:%s" % self.sub_dict)
            self._status = True

    def stop(self):
        self.quote_ctx.stop()
        self._status = False
        logger.debug("stop success")

    def close(self):
        self.quote_ctx.close()
        logger.debug("close success")

    def status(self):
        with self.lock:
            return self._status

    def get_order_book_data(self, code):
        ret, data = self.quote_ctx.get_order_book(code)
        return ret, data

    def get_tick_data(self, code):
        ret, data = self.quote_ctx.get_rt_ticker(code)
        return ret, data

    def get_quote_data(self, code):
        ret, data = self.quote_ctx.get_stock_quote(code)
        return ret, data

    def subscribe(self, code_list, dtype, callback = None):
        if dtype in self.sub_dict and set(code_list).issubset(set(self.sub_dict[dtype])): return 0
        if callback is not None: self.quote_ctx.set_handler(callback)
        ret, msg = self.quote_ctx.subscribe(code_list, dtype)
        if 0 == ret:
            if dtype not in self.sub_dict: self.sub_dict[dtype] = list()
            self.sub_dict[dtype].extend(code_list)
        else:
            logger.error("%s subscrbe failed, msg:%s, dtype:%s" % (code, msg, dtype))
        return ret

    def unsubscribe(self, code_list, subtype):
        '''
        code_list – 取消订阅的股票代码列表
        subtype_list – 取消订阅的类型，参见SubType
        '''
        ret, msg = self.quote_ctx.unsubscribe(code_list, subtype)
        if 0 == ret:
            if subtype in self.sub_dict and code in self.sub_dict[subtype]:
                self.sub_dict[subtype].remove(code)
        else:
            logger.error(msg)
        return ret

    def get_subscribed_dict(self):
        ret, data = self.quote_ctx.query_subscription()
        if 0 != ret: 
            logger.error(msg)
            return None
        return data['sub_dict'] if 'sub_dict' in data else dict()

if __name__ == '__main__':
    from common import get_index_list
    s = Subscriber()
    s.start()
    x = s.subscribe_quote()
    print(x)
    while True:
        ret, data = s.get_quote_data(get_index_list())
        print(ret, data)
