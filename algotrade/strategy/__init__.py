# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from cindex import CIndex
from common import is_df_has_unexpected_data
from pyalgotrade.broker import slippage, fillstrategy, Order
from algotrade.broker.futu.backtestbroker import Broker, TradePercentage
def gen_broker(feed, cash = 100000, trade_percent = 0.01, volume_limit = 0.01, market = ct.CN_MARKET_SYMBOL):
    # cash：初始资金
    # trade_percent: 手续费, 每笔交易金额的百分比
    # volume_limit: 每次交易能成交的量所能接受的最大比例
    # Broker Setting
    # Broker Commission类设置
    broker_commission = TradePercentage(trade_percent)
    # Fill Strategy设置
    fill_stra = fillstrategy.DefaultStrategy(volumeLimit = volume_limit)
    sli_stra = slippage.NoSlippage()
    fill_stra.setSlippageModel(sli_stra)
    # 完善Broker类
    brk = Broker(cash, feed, market, broker_commission)
    brk.setFillStrategy(fill_stra)
    return brk

def get_data(code, start_date, end_date):
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data_in_range(start_date, end_date)
    data = data.set_index('date')
    if is_df_has_unexpected_data(data): return None
    data.index = pd.to_datetime(data.index)
    data = data.dropna(how='any')
    return data
