# coding=utf-8
import json
import time
import datetime
import pandas as pd
import tushare as ts
from trader import Trader
from log import getLogger
import const as ct

STOCK_NUM = 30
IPO_DATE_HEAD = 'ipo_date'
IPO_CODE_HEAD = 'xcode'
IPO_PRICE_HEAD = 'price'

def buy_new_stock():
    with open(ct.USER_FILE) as f:
        infos = json.load(f)
    trader = Trader(infos[0]["account"], infos[0]["passwd_encrypted"], infos[0]["secuids_sh"], infos[0]["secuids_sz"])
    for stock in get_new_stock_list():
        time.sleep(10)
        ret, amount = trader.max_amounts(stock[0], stock[1])
        if 0 == ret:
            time.sleep(10)
            quantAI.deal(stock[0], stock[1], amount, "B")

def get_new_stock_list():
    stock_list = []
    top_stocks_info = ts.new_stocks().head(STOCK_NUM)
    stocks_info = top_stocks_info[[IPO_CODE_HEAD, IPO_DATE_HEAD, IPO_PRICE_HEAD]]
    for i in range(STOCK_NUM):
        stock_date = stocks_info.at[i, IPO_DATE_HEAD]
        if pd.to_datetime(stock_date).strftime('%Y-%m-%d') == datetime.datetime.now().strftime('%Y-%m-%d'):
            code = stocks_info.at[i, IPO_CODE_HEAD]
            price = stocks_info.at[i, IPO_PRICE_HEAD]
            stock_list.append(tuple(code, price))
    return stock_list

if __name__ == "__main__":
    buy_new_stock()
