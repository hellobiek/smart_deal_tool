# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from common import get_market
from datamanager.hgt import StockConnect
class HGT(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.sh_df = None
        self.sz_df = None
        self.sh_connect_client = StockConnect(market_from = ct.SH_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL, dbinfo = dbinfo, redis_host = redis_host)
        self.sz_connect_client = StockConnect(market_from = ct.SZ_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL, dbinfo = dbinfo, redis_host = redis_host)

    def update(self, start_date, end_date):
        self.sh_df = self.get_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
        self.sz_df = self.get_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)

    def get_top10_info(self, cdate):
        sh_info = self.sh_connect_client.get_top10_stocks(cdate)
        sz_info = self.sz_connect_client.get_top10_stocks(cdate)
        sh_info = sh_info.append(sz_info)
        return sh_info

    def get_data(self, code, start_date, end_date):
        if code == ct.SH_MARKET_SYMBOL:
            df = self.sh_connect_client.get_k_data(dtype = ct.HGT_CAPITAL)
            if df is None: return
            total_buy = 294598
            df['net_buy'] = df['buy_turnover'] - df['sell_turnover']
            df['cum_buy'] = df['net_buy'].cumsum()
            df['cum_buy'] = df['cum_buy'] + total_buy
            df['cum_buy'] = df['cum_buy'] / 100
            df['net_buy'] = df['net_buy'] / 100
            df = df.loc[(df.date >= start_date) & (df.date <= end_date)]
            df = df.reset_index(drop = True)
        elif code == ct.SZ_MARKET_SYMBOL:
            df = self.sz_connect_client.get_k_data(dtype = ct.HGT_CAPITAL)
            if df is None: return
            total_buy = 231568
            df['net_buy'] = df['buy_turnover'] - df['sell_turnover']
            df['cum_buy'] = df['net_buy'].cumsum()
            df['cum_buy'] = df['cum_buy'] + total_buy
            df['cum_buy'] = df['cum_buy'] / 100
            df['net_buy'] = df['net_buy'] / 100
            df = df.loc[(df.date >= start_date) & (df.date <= end_date)]
            df = df.reset_index(drop = True)
        else:
            if code == "ALL_SH":
                df = self.sh_connect_client.get_k_data_in_range(start_date, end_date, dtype = ct.HGT_STOCK)
            elif code == "ALL_SZ":
                df = self.sz_connect_client.get_k_data_in_range(start_date, end_date, dtype = ct.HGT_STOCK)
            else:
                if get_market(code) == ct.MARKET_SH:
                    df = self.sh_connect_client.get_k_data_in_range(start_date, end_date, dtype = ct.HGT_STOCK)
                else:
                    df = self.sz_connect_client.get_k_data_in_range(start_date, end_date, dtype = ct.HGT_STOCK)
                df = df.loc[df.code == code]
                df = df.reset_index(drop = True)
                df['delta'] = df['percent'] - df['percent'].shift(1)
            if df is None: return
        return df

if __name__ == "__main__":
    hgt = HGT()
    hgt.get_data('601318', '2019-01-01', '2019-09-18')
