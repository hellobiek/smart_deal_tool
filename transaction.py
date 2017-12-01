#!/usr/local/bin/python3
# -*- coding:utf-8 -*-
from const import MARKET_SH, MARKET_SZ, MARKET_ELSE

class Stock:
    def __init__(self, code, name="" ,price = 0):
        self.code = code 
        self.price = price
        self.name = name
        self.market = self.market()
        #self.industry = ""
        #self.area = ""
        #self.pe = 0
        #self.pb = 0
        #self.totals = 0
        #self.outstanding = 0
        #self.totalAssets = 0
        #self.liquidAssets = 0
        #self.fixedAssets = 0
        #self.reserved = 0
        #self.reservedPerShare = 0
        #self.eps = 0
        #self.bvps = 0
        #self.timeToMarket = 0
        #self.stockOwnerNum = 0

    def market(self):
        if (self.code.startswith("7") or self.code.startswith("6") or self.code.startswith("500") or self.code.startswith("550") or self.code.startswith("510")):
            return MARKET_SH
        elif (self.code.startswith("00") or self.code.startswith("30") or self.code.startswith("150") or self.code.startswith("159")):
            return MARKET_SZ
        else:
            return MARKET_ELSE

class HoldedStock(Stock):
    def __init__(self, code, price = 0, buy_price = 0, amount = 0):
        Stock.__init__(code, price)
        self.amount = amount
        self.buy_price = buy_price
