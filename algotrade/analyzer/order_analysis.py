import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from common import get_dates_array
from futu import TrdEnv
from algotrade.broker.futu.fututrader import FutuTrader, MOrder, MDeal

def main():
    unlock_path_ = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/futu.json"
    futuTrader = FutuTrader(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.REAL, market = ct.US_MARKET_SYMBOL, unlock_path = unlock_path_)
    start = '2019-02-25' 
    end   = '2019-03-01'
    date_arrary = get_dates_array(start, end, dformat = "%Y-%m-%d", asending = True)
    for cdate in date_arrary:
        orders = futuTrader.get_history_orders(start = cdate, end = cdate)
        orders = orders[['code', 'trd_side', 'order_id', 'dealt_qty', 'dealt_avg_price', 'create_time']]
        print(orders)
        import pdb
        pdb.set_trace()

if __name__ == "__main__": 
    main()
