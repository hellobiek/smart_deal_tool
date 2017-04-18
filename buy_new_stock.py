# coding=utf-8
import json,time,datetime
import pandas as pd
import tushare as ts
from stock import Stock
from stock_ai import StockAI
from log import getLogger
from const import USER_FILE,NEW_STOCK_URL,RETRY_TIMES,SLEEP_TIME

STOCK_NUM = 100
IPO_DATE_HEAD = 'ipo_date'
IPO_CODE_HEAD = 'xcode'
IPO_PRICE_HEAD = 'price'

def main():
    for quant in get_all_quants():
        quantAI = StockAI(quant["account"], quant["passwd_encrypted"], quant["secuids_sh"], quant["secuids_sz"])
        for stock in get_new_stock_list():
            time.sleep(15)
            ret, amount = quantAI.amounts(stock)
            if ret == 0:
                time.sleep(15)
                print quantAI.deal(stock, stock.price, amount, "B")
            else:
                print "get amount error"

def get_all_quants():
	with open(USER_FILE) as f:
		return json.load(f)

def get_new_stock_list():
    stock_list = []
    top_stocks_info = ts.new_stocks().head(STOCK_NUM)
    stocks_info = top_stocks_info[[IPO_CODE_HEAD,IPO_DATE_HEAD,IPO_PRICE_HEAD]]
    for i in xrange(STOCK_NUM):
        stock_date = stocks_info.at[i, IPO_DATE_HEAD]
        if pd.to_datetime(stock_date).strftime('%Y-%m-%d') == datetime.datetime.now().strftime('%Y-%m-%d'):
            stock_code = stocks_info.at[i, IPO_CODE_HEAD]
            stock_price = stocks_info.at[i, IPO_PRICE_HEAD]
            stock_list.append(Stock(stock_code, name = "", price = stock_price))
    return stock_list

if '__main__' == __name__:
    main()
