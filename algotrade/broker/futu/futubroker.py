#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(dirname(abspath(__file__))))))
import time
import queue
import random
import datetime
import threading
import const as ct
from base.clog import getLogger
from pyalgotrade import broker
from base.base import get_today_time, localnow
from algotrade.broker.futu.fututrader import FutuTrader, MOrder, MDeal
from futu import OrderStatus, OrderType, TrdEnv, TradeDealHandlerBase, TradeOrderHandlerBase
logger = getLogger(__name__)
class EquityTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return int(quantity)
    def roundPrice(self, price):
        return round(price, 2)

class LiveOrder(object):
    def __init__(self):
        self.__accepted = None

    def setAcceptedDateTime(self, dateTime):
        self.__accepted = dateTime

    def getAcceptedDateTime(self):
        return self.__accepted

    # Override to call the fill strategy using the concrete order type.
    # return FillInfo or None if the order should not be filled.
    def process(self, broker_, bar_):
        raise NotImplementedError()

class MarketOrder(broker.MarketOrder, LiveOrder):
    def __init__(self, action, instrument, quantity, onClose, instrumentTraits):
        broker.MarketOrder.__init__(self, action, instrument, quantity, onClose, instrumentTraits)
        LiveOrder.__init__(self)

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillMarketOrder(broker_, self, bar_)

class LimitOrder(broker.LimitOrder, LiveOrder):
    def __init__(self, action, instrument, limitPrice, quantity, instrumentTraits):
        broker.LimitOrder.__init__(self, action, instrument, limitPrice, quantity, instrumentTraits)
        LiveOrder.__init__(self)

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillLimitOrder(broker_, self, bar_)

class StopOrder(broker.StopOrder, LiveOrder):
    def __init__(self, action, instrument, stopPrice, quantity, instrumentTraits):
        broker.StopOrder.__init__(self, action, instrument, stopPrice, quantity, instrumentTraits)
        LiveOrder.__init__(self)
        self.__stopHit = False

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillStopOrder(broker_, self, bar_)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit

# http://www.sec.gov/answers/stoplim.htm
# http://www.interactivebrokers.com/en/trading/orders/stopLimit.php
class StopLimitOrder(broker.StopLimitOrder, LiveOrder):
    def __init__(self, action, instrument, stopPrice, limitPrice, quantity, instrumentTraits):
        broker.StopLimitOrder.__init__(self, action, instrument, stopPrice, limitPrice, quantity, instrumentTraits)
        LiveOrder.__init__(self)
        self.__stopHit = False  # Set to true when the limit order is activated (stop price is hit)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit

    def isLimitOrderActive(self):
        # TODO: Deprecated since v0.15. Use getStopHit instead.
        return self.__stopHit

    def process(self, broker_, bar_):
        return broker_.getFillStrategy().fillStopLimitOrder(broker_, self, bar_)

class TradeDealHandler(TradeOrderHandlerBase):
    def __init__(self):
        super(TradeDealHandler, self).__init__()
        self.__lock  = threading.Lock()
        self.__queue = queue.Queue()

    def empty(self):
        with self.__lock:
            return self.__queue.empty()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_pb):
        ret, data = super(TradeDealHandler, self).on_recv_rsp(rsp_pb)
        if ret != 0: return
        data = data[['order_id', 'order_status', 'trd_side', 'code', 'qty', 'price', 'create_time', 'trd_env']]
        data_dict = data.to_dict("records")
        with self.__lock:
            for mdict in data_dict:
                if mdict['order_status'] in ["FILLED_PART", "FILLED_ALL"]:
                    self.__queue.put(MDeal(mdict))

class FutuBroker(broker.Broker):
    def __init__(self, host, port, trd_env, timezone, dealtime, order_type = OrderType.NORMAL, market = "CN", unlock_path = ct.FUTU_PATH):
        super(FutuBroker, self).__init__()
        self.__stop          = False
        self.__trader        = None 
        self.__cash          = 0
        self.__shares        = dict()
        self.__host          = host
        self.__port          = port
        self.__market        = market
        self.__trd_env       = trd_env
        self.__order_type    = order_type
        self.__timezone      = timezone
        self.__start_time    = get_today_time(dealtime['start'])
        self.__end_time      = get_today_time(dealtime['end'])
        self.__activeOrders  = dict()
        self.__unlock_path   = unlock_path
        self.__deal_manager  = TradeDealHandler()

    def update_account_balance(self):
        self.__stop   = True
        self.__cash   = self.__trader.get_cash()
        self.__shares = self.__trader.get_shares()
        self.__stop   = False

    def _registerOrder(self, order):
        pass

    def _unregisterOrder(self, order):
        pass

    # BEGIN observer.Subject interface
    def start(self):
        super(FutuBroker, self).start()
        self.__trader = FutuTrader(self.__host, self.__port, self.__trd_env, self.__market, unlock_path = self.__unlock_path)
        self.__trader.set_handler(self.__deal_manager)
        self.__trader.start()
        self.update_account_balance()

    def stop(self):
        self.__stop = True
        self.__trader.close()

    def join(self):
        pass

    def eof(self):
        if localnow(self.__timezone) >= self.__end_time:
            self.stop()
        return self.__stop

    def dispatch(self):
        if not self.__deal_manager.empty():
            self.__deal_manager.getQueue()
            self.update_account_balance()

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None
    # END observer.Subject interface

    # BEGIN broker.Broker interface
    def getCash(self, includeShort=True):
        return self.__cash

    def getInstrumentTraits(self, instrument):
        return EquityTraits()

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        return self.__shares

    def getActiveOrders(self, instrument=None):
        raise Exception("get active orders are not supported")

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        instrumentTraits = self.getInstrumentTraits(instrument)
        order = LimitOrder(action, instrument, limitPrice, quantity, instrumentTraits)
        order.setAllOrNone(False)
        order.setGoodTillCanceled(False)
        return order

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        raise Exception("market orders are not supported")

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("stop limit orders are not supported")

    def submitOrder(self, order):
        ret, data = self.__trader.trade(order)
        if ret != 0: logger.error("submit order failed. ret:%s, output:%s" % (ret, data))

    def cancelOrder(self, order):
        raise Exception("cancel orders are not supported")
    # END broker.Broker interface
