#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(dirname(abspath(__file__))))))
import queue
import threading
import const as ct
from pyalgotrade import broker
from common import add_prifix
from base.clog import getLogger
from base.base import get_today_time, localnow
from futu import OrderType, TradeDealHandlerBase, TradeOrderHandlerBase
from algotrade.broker.futu.fututrader import FutuTrader, MOrder
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

class TradeOrderHandler(TradeOrderHandlerBase):
    def __init__(self):
        super(TradeOrderHandler, self).__init__()
        self.__lock  = threading.Lock()
        self.__queue = queue.Queue()

    def empty(self):
        with self.__lock:
            return self.__queue.empty()

    def getQueue(self):
        with self.__lock:
            return self.__queue

    def on_recv_rsp(self, rsp_pb):
        ret, data = super(TradeOrderHandler, self).on_recv_rsp(rsp_pb)
        if ret != 0: return
        data = data[['order_id', 'order_type', 'order_status', 'trd_side', 'code', 'qty',
                     'price', 'dealt_avg_price', 'dealt_qty', 'create_time', 'updated_time']]
        data_dict = data.to_dict("records")
        with self.__lock:
            for mdict in data_dict:
                if mdict['order_status'] in ["FILLED_PART", "FILLED_ALL"]:
                    self.__queue.put(MOrder(mdict))

def build_order_from_open_order(order, instrumentTraits):
    if order.isBuy():
        action = broker.Order.Action.BUY
    elif order.isSell():
        action = broker.Order.Action.SELL
    else:
        raise Exception("Invalid order type")
    ret = broker.LimitOrder(action, order.getInstrument(), order.getPrice(), order.getAmount(), instrumentTraits)
    ret.setSubmitted(order.getId(), order.getDateTime())
    ret.setState(broker.Order.State.ACCEPTED)
    return ret

class FutuBroker(broker.Broker):
    def __init__(self, host, port, trd_env, timezone, dealtime, order_type = OrderType.NORMAL, market = "CN", unlock_path = ct.FUTU_PATH):
        super(FutuBroker, self).__init__()
        self.__cash          = 0
        self.__tassert       = 0
        self.__host          = host
        self.__port          = port
        self.__trader        = None
        self.__stop          = False
        self.__shares        = dict()
        self.__positons      = None
        self.__activeOrders  = dict()
        self.__market        = market
        self.__trd_env       = trd_env
        self.__timezone      = timezone
        self.__order_type    = order_type
        self.__unlock_path   = unlock_path
        self.__deal_manager  = TradeOrderHandler()
        self.__logger        = getLogger(__name__)
        self.__start_time    = get_today_time(dealtime['start'])
        self.__end_time      = get_today_time(dealtime['end'])

    def refresh_open_orders(self):
        self.__stop = True  # Stop running in case of errors.
        self.__logger.info("get open orders.")
        orders = self.__trader.get_open_orders()
        for order in orders:
            self._registerOrder(build_order_from_open_order(order, self.getInstrumentTraits(order.getInstrument())))
        self.__logger.info("{} open order found".format(len(orders)))
        self.__stop = False  # No errors. Keep running.

    def update_account_balance(self):
        self.__stop   = True
        self.__cash   = self.__trader.get_cash()
        self.__shares = self.__trader.get_shares()
        self.__positons = self.__trader.get_postitions()
        self.__tassert = self.__trader.get_total_assets()
        self.__stop   = False

    def _registerOrder(self, order):
        assert(order.getId() is not None)
        assert(order.getId() not in self.__activeOrders)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert(order.getId() is not None)
        assert(order.getId() in self.__activeOrders)
        del self.__activeOrders[order.getId()]

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

    def _on_user_trades(self, morder):
        order = self.__activeOrders.get(morder.getId())
        if order is not None:
            # Update the order.
            orderExecutionInfo = broker.OrderExecutionInfo(morder.getDealtAvgPrice(), morder.getDealtQuantity(), 0, morder.getUpdatedTime())
            order.addExecutionInfo(orderExecutionInfo)
            if not order.isActive():
                self._unregisterOrder(order)
            # Notify that the order was updated.
            if order.isFilled():
                eventType = broker.OrderEvent.Type.FILLED
            else:
                eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
            self.notifyOrderEvent(broker.OrderEvent(order, eventType, orderExecutionInfo))
        else:
            self.__logger.info("order {} refered to futu order {} that is not active".format(order.getId(), morder.getId()))

    def dispatch(self):
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = list(self.__activeOrders.values())
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

        if not self.__deal_manager.empty():
            morder = self.__deal_manager.getQueue().get(True, ct.QUEUE_TIMEOUT)
            self._on_user_trades(morder)
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

    def getInstrumentName(self, code):
        if self.__market == ct.CN_MARKET_SYMBOL:
            return add_prifix(code)
        else:
            return "{}.{}".format(self.__market, code)

    def getPositions(self):
        return self.__positons

    def getEquity(self):
        return self.__tassert

    def getActiveOrders(self, instrument=None):
        return list(self.__activeOrders.values())

    def createLimitOrder(self, action, code, limitPrice, quantity):
        limitPrice = round(limitPrice, 2)
        instrumentTraits = self.getInstrumentTraits(code)
        instrument = self.getInstrumentName(code)
        quantity = instrumentTraits.roundQuantity(quantity)
        return LimitOrder(action, instrument, limitPrice, quantity, instrumentTraits)

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        raise Exception("market orders are not supported")

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("stop limit orders are not supported")

    def submitOrder(self, order, model):
        if order.isInitial():
            # Override user settings based on Bitstamp limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(False)
            ret, data = self.__trader.trade(order, model)
            if ret != 0:
                errMsg = "submit order failed. ret:{}, output:{}".format(ret, data)
                self.__logger.error(errMsg)
            else:
                forder = data.to_dict('records')[0]
                order.setSubmitted(forder['order_id'], forder['create_time'])
                self._registerOrder(order)
                # Switch from INITIAL -> SUBMITTED
                # IMPORTANT: Do not emit an event for this switch because when using the position interface
                # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
                order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        ret, _ = self.__trader.cancel(order.getId())
        if ret == 0:
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            # Update cash and shares.
            self.update_account_balance()
            # Notify that the order was canceled.
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))
    # END broker.Broker interface
