#-*-coding:utf-8-*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(dirname(abspath(__file__))))))
import time
import queue
import random
import datetime
import threading
import const as ct
from pyalgotrade import broker
from algotrade.broker.futu.fututrader import FutuTrader, MOrder, MDeal
from futuquant import OrderStatus, OrderType, TrdEnv, TradeDealHandlerBase, TradeOrderHandlerBase, TrdSide
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

class TradeDealHandler(TradeDealHandlerBase):
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
        data = data[['deal_id', 'trd_side', 'order_id', 'code', 'qty', 'price', 'create_time']]
        data_dict = data.to_dict("records")
        with self.__lock:
            for mdict in data_dict:
                self.__queue.put(MDeal(mdict))

class TradeOrderHandler(TradeOrderHandlerBase):
    def __init__(self):
        super(TradeOrderHandler, self).__init__()
        self.__lock  = threading.Lock()
        self.__queue = queue.Queue()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_pb):
        ret, data = super(TradeOrderHandler, self).on_recv_rsp(rsp_pb)
        if ret != 0: return
        data = data[['order_id', 'order_type', 'order_status', 'code', 'trd_side', 'qty', 'price', 'create_time', 'dealt_qty', 'dealt_avg_price', 'updated_time']]
        data_dict = data.to_dict("records")
        with self.__lock:
            for mdict in data_dict:
                self.__queue.put(MOrder(mdict))

class FutuBroker(broker.Broker, TradeDealHandlerBase, TradeOrderHandlerBase):
    def __init__(self, host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, order_type = OrderType.NORMAL, market = "CN"):
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
        self.__activeOrders  = dict()
        self.__deal_manager  = TradeDealHandler()
        self.__order_manager = TradeOrderHandler()

    def update_account_balance(self):
        self.__stop   = True
        self.__cash   = self.__trader.get_cash()
        self.__shares = self.__trader.get_shares()
        self.__stop   = False

    def init_orders(self):
        self.__stop = True
        for morder in self.__trader.get_order(filter_list = ["SUBMITTED"]):
            self.create_order(morder)
        self.__stop = False

    def create_order(self, morder):
        code     = morder.getInstrument()
        ctime    = morder.getCreateTime()
        id_      = morder.getId()
        type_    = morder.getType()
        price    = morder.getPrice()
        status   = morder.getStatus()
        quantity = morder.getQuantity()
        action = broker.Order.Action.BUY if morder.getAction() == 'BUY' else broker.Order.Action.SELL
        order = self.createLimitOrder(action, code, price, quantity)
        order.setSubmitted(id_, ctime)
        if status == OrderStatus.UNSUBMITTED or status == OrderStatus.SUBMITTING or OrderStatus.SUBMIT_FAILED or OrderStatus.WAITING_SUBMIT: 
            order.setState(broker.Order.State.INITIAL)
        elif status == OrderStatus.SUBMITTED:
            order.setState(broker.Order.State.SUBMITTED)
        elif status == OrderStatus.FILLED_PART:
            order.setState(broker.Order.State.FILLED_PART)
        elif status == OrderStatus.FILLED_ALL:
            order.setState(broker.Order.State.FILLED)
        return order

    def update_order(self, morder):
        id_    = morder.getId()
        status = morder.getStatus()
        order = self.__activeOrders.get(id_)
        if order is None:
            if status == OrderStatus.SUBMITTED or status == OrderStatus.FILLED_PART:
                order = self.create_order(morder)
                self._registerOrder(order)
        else:
            if morder_status == OrderStatus.UNSUBMITTED:
            elif morder_status == OrderStatus.SUBMITTING:
            elif morder_status == OrderStatus.SUBMIT_FAILED:
            elif morder_status == OrderStatus.WAITING_SUBMIT:

            elif morder_status == OrderStatus.SUBMITTED:

            elif morder_status == OrderStatus.FILLED_PART:

            elif morder_status == OrderStatus.FILLED_ALL:

            elif morder_status == OrderStatus.CANCELLING_PART:
            elif morder_status == OrderStatus.CANCELLING_ALL:
            elif morder_status == OrderStatus.CANCELLED_PART:
            elif morder_status == OrderStatus.CANCELLED_ALL:
            elif morder_status == OrderStatus.FAILED:
            elif morder_status == OrderStatus.DISABLED:
            elif morder_status == OrderStatus.DELETED:
            else:
                raise Exception("unhandled order status:%s" % status)

    def _registerOrder(self, order):
        assert(order.getId() not in self.__activeOrders)
        assert(order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert(order.getId() in self.__activeOrders)
        assert(order.getId() is not None)
        del self.__activeOrders[order.getId()]

    # BEGIN observer.Subject interface
    def start(self):
        super(FutuBroker, self).start()
        self.__trader = FutuTrader(self.__host, self.__port, self.__trd_env, self.__market)
        self.__trader.set_handler(TradeDealHandler())
        self.__trader.set_handler(TradeOrderHandler())
        self.update_account_balance()
        self.init_orders()
        self.__trader.buy(code = "SH.601988", price = 3.4, quantity = 100)

    def stop(self):
        self.__stop = True
        self.__trader.close()

    def join(self):
        pass

    def eof(self):
        return self.__stop

    def dispatch(self):
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = self.__activeOrders.values()
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

        if not self.__deal_manager.empty():
            deal_queue = self.__deal_manager.getQueue()
            while not deal_queue.empty():
                mdeal = deal_queue.get()
                self.update_account_balance()

        if not self.__order_manager.empty():
            order_queue = self.__order_manager.getQueue()
            while not order_queue.empty():
                morder = order_queue.get()
                self.update_order(morder)

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
        return self.__activeOrders.values()

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        instrumentTraits = self.getInstrumentTraits(instrument)
        order = broker.LimitOrder(action, instrument, limitPrice, quantity, instrumentTraits)
        order.setAllOrNone(False)
        order.setGoodTillCanceled(True)
        return order

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        raise Exception("Market orders are not supported")

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("Stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("Stop limit orders are not supported")

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")
        ret, data = self.__trader.modify(order, 'cancel')
        if ret != 0: Exception("The order is canceled failed")
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)
        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))
    # END broker.Broker interface

if __name__ =="__main__":
    fb = FutuBroker()
    fb.start()
