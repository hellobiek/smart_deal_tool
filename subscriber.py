#-*-coding:utf-8-*-
import const as ct
from log import getLogger
from gevent.lock import Semaphore
from futuquant import OpenQuoteContext
from futuquant.common.constant import SubType
from futuquant.quote.quote_response_handler import OrderBookHandlerBase, TickerHandlerBase, StockQuoteHandlerBase
logger = getLogger(__name__)

class StockQuoteHandler(StockQuoteHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret, data = super(StockQuoteHandler, self).on_recv_rsp(rsp_str)
        return ret, data

class TickerHandler(TickerHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret, data = super(TickerHandler, self).on_recv_rsp(rsp_pb)
        return ret, data

class OrderBookHandler(OrderBookHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret, data = super(OrderBookHandler, self).on_recv_rsp(rsp_pb)
        return ret, data

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

    def subscribe_order_book(self, code, callback):
        if SubType.ORDER_BOOK in self.sub_dict and code in self.sub_dict[SubType.ORDER_BOOK]: return 0
        self.quote_ctx.set_handler(callback)
        ret, msg = self.quote_ctx.subscribe(code, SubType.ORDER_BOOK)
        if 0 == ret:
            if SubType.ORDER_BOOK not in self.sub_dict: self.sub_dict[SubType.ORDER_BOOK] = list()
            self.sub_dict[SubType.ORDER_BOOK].append(code)
        else:
            logger.error("%s subscrbe failed, msg:%s" % (code, msg))
        return ret

    def subscribe_tick(self, code, callback):
        if SubType.TICKER in self.sub_dict and code in self.sub_dict[SubType.TICKER]: return 0
        self.quote_ctx.set_handler(callback)
        ret, msg = self.quote_ctx.subscribe(code, SubType.TICKER)
        if 0 == ret:
            if SubType.TICKER not in self.sub_dict: self.sub_dict[SubType.TICKER] = list()
            self.sub_dict[SubType.TICKER].append(code)
        else:
            logger.error("%s subscrbe failed, msg:%s" % (code, msg))
        return ret

    def subscribe_quote(self, code, callback):
        if SubType.QUOTE in self.sub_dict and code in self.sub_dict[SubType.QUOTE]: return 0
        self.quote_ctx.set_handler(callback)
        ret, msg = self.quote_ctx.subscribe(code, SubType.QUOTE)
        if 0 == ret:
            if SubType.QUOTE not in self.sub_dict: self.sub_dict[SubType.QUOTE] = list()
            self.sub_dict[SubType.QUOTE].append(code)
        else:
            logger.error("%s subscrbe failed, msg:%s" % (code, msg))
        return ret

    def unsubscribe_tick(self, code_list, subtype = SubType.TICKER):
        '''
        code_list – 取消订阅的股票代码列表
        subtype_list – 取消订阅的类型，参见SubType
        '''
        ret, msg = self.quote_ctx.unsubscribe(code_list, subtype)
        if 0 == ret:
            if SubType.TICKER in self.sub_dict and code in self.sub_dict[SubType.TICKER]:
                self.sub_dict[SubType.TICKER].remove(code)
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
