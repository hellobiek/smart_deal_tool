# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from datamanager.hgt import StockConnect
class HGT(object):
    def __init__(self):
        self.sh_df = None
        self.sz_df = None
        self.sh_connect_client = StockConnect(market_from = ct.SH_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL)
        self.sz_connect_client = StockConnect(market_from = ct.SZ_MARKET_SYMBOL, market_to = ct.HK_MARKET_SYMBOL)

    def update(self, start_date, end_date):
        self.sh_df = self.get_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
        self.sz_df = self.get_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)

    def get_data(self, market, start_date, end_date):
        if market == ct.SH_MARKET_SYMBOL:
            df = self.sh_connect_client.get_k_data(dtype = ct.HGT_CAPITAL)
            total_buy = 294598
        else:
            df = self.sz_connect_client.get_k_data(dtype = ct.HGT_CAPITAL)
            total_buy = 231568
        df['net_buy'] = df['buy_turnover'] - df['sell_turnover']
        df['net_buy'] = df['buy_turnover'] - df['sell_turnover']
        df['cum_buy'] = df['net_buy'].cumsum()
        df['cum_buy'] = df['cum_buy'] + total_buy
        df['cum_buy'] = df['cum_buy'] / 100
        df = df.loc[(df.date >= start_date) & (df.date <= end_date)]
        df = df.reset_index(drop = True)
        return df

