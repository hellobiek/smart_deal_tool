import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from cindex import CIndex
from pyalgotrade import broker
from common import is_df_has_unexpected_data
def gen_broker(feed, cash = 10000000, trade_percent = 0.01, volume_limit = 0.01):
    # cash：初始资金
    # trade_percent: 手续费, 每笔交易金额的百分比
    # volume_limit: 每次交易能成交的量所能接受的最大比例
    # Broker Setting
    # Broker Commission类设置
    broker_commission = broker.backtesting.TradePercentage(trade_percent)
    # Fill Strategy设置
    fill_stra = broker.fillstrategy.DefaultStrategy(volumeLimit = volume_limit)
    sli_stra = broker.slippage.NoSlippage()
    fill_stra.setSlippageModel(sli_stra)
    # 完善Broker类
    brk = broker.backtesting.Broker(cash, feed, broker_commission)
    brk.setFillStrategy(fill_stra)
    return brk

def get_data(code, start_date, end_date):
    cstock_obj = CIndex(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = cstock_obj.get_k_data_in_range(start_date, end_date)
    data = data.set_index('date')
    if is_df_has_unexpected_data(data): return None
    data.index = pd.to_datetime(data.index)
    data = kdj(data)
    data = data.dropna(how='any')
    return data
