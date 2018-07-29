# -*- coding: utf-8 -*-
import futuquant as ft
from futuquant import *

def enum_all_index(ip, port):
    quote_ctx = ft.OpenQuoteContext(ip, port)
    ret, data_frame = quote_ctx.get_stock_basicinfo(market=ft.Market.SH, stock_type=ft.SecurityType.IDX)
    data_frame.to_csv("index_sh.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market SH index data saved!')
    ret, data_frame = quote_ctx.get_stock_basicinfo(market=ft.Market.SZ, stock_type=ft.SecurityType.IDX)
    data_frame.to_csv("index_sz.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market SZ index data saved!')
    ret, data_frame = quote_ctx.get_stock_basicinfo(market=ft.Market.HK, stock_type=ft.SecurityType.IDX)
    data_frame.to_csv("index_hk.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market HK index data saved!')
    ret, data_frame = quote_ctx.get_stock_basicinfo(market=ft.Market.US, stock_type=ft.SecurityType.IDX)
    data_frame.to_csv("index_us.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market US index data saved!')
    quote_ctx.close()

def get_index_stocks(ip, port, code):
    quote_ctx = ft.OpenQuoteContext(ip, port)
    ret, data_frame = quote_ctx.get_plate_stock(code)
    quote_ctx.close()
    return ret, data_frame

if __name__ == "__main__":
    api_ip = 'host.docker.internal'
    api_port = 11111
    quote_ctx = OpenQuoteContext(host=api_ip, port=api_port)
    #print(quote_ctx.get_market_snapshot('US.BABA'))
    #print(quote_ctx.subscribe(['US.BABA'], [SubType.QUOTE]))
    #print(quote_ctx.get_rt_data('US.BABA'))
    #print(quote_ctx.get_rt_ticker('US.BABA', num = 500))
    #print(quote_ctx.get_order_book('US.BABA'))

    #today = datetime.datetime.today()
    #pre_day = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    #end_dt = today.strftime('%Y-%m-%d')
    ret_code, prices = quote_ctx.get_history_kline('SH.601318', start='2018-07-25', end='2018-07-25', ktype=ft.KLType.K_DAY)
    print(prices)

    #print(quote_ctx.get_stock_basicinfo(Market.US, SecurityType.STOCK))
    quote_ctx.close()
    #enum_all_index(api_ip, api_port)
    #print('SH.000001 上证指数 \n')
    #print(get_index_stocks(api_ip, api_port, 'SH.000001'))

    #print('SZ.399006 创业板指\n')
    #print(get_index_stocks(api_ip, api_port, 'SZ.399006'))

    #print('HK.800000 恒生指数 \n')
    #print(get_index_stocks(api_ip, api_port, 'HK.800000'))

    #print('US..DJI 道琼斯指数\n')
    #print(get_index_stocks(api_ip, api_port, 'US..DJI'))
