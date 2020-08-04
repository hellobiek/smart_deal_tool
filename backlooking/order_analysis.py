#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import datetime
import const as ct
import pandas as pd
from futu import TrdEnv
from datetime import datetime
from base.cdate import get_dates_array
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
from algotrade.broker.futu.fututrader import FutuTrader
pd.options.mode.chained_assignment = None
def get_total_profit(orders):
    buy_orders  = orders.loc[orders.trd_side == 'BUY']
    buy_orders  = buy_orders.reset_index(drop = True)
    sell_orders = orders.loc[orders.trd_side == 'SELL']
    sell_orders = sell_orders.reset_index(drop = True)
    total_sell_value = (sell_orders['dealt_qty'] * sell_orders['dealt_avg_price']).sum()
    total_buy_value  = (buy_orders['dealt_qty'] * buy_orders['dealt_avg_price']).sum()
    return total_sell_value - total_buy_value

def generate(orders, date_arrary, dirname, start, end):
    filename = 'form_%s_to_%s_tading_review.md' % (start, end) 
    os.makedirs(dirname, exist_ok = True)
    fullfilepath = os.path.join(dirname, filename)
    orders = orders[['code', 'trd_side', 'dealt_qty', 'dealt_avg_price', 'create_time', 'updated_time']]
    total_profit = get_total_profit(orders)
    md = MarkdownWriter()
    md.addTitle("%s_%s_交割单" % (start, end), passwd = '909897')
    md.addHeader("交割单分析", 1)
    md.addHeader("总收益分析", 2)
    t_index = MarkdownTable(headers = ["总收益"])
    t_index.addRow(["%s" % total_profit])
    md.addTable(t_index)
    md.addHeader("交割单复盘", 2)
    for cdate in date_arrary:
        md.addHeader("%s_交割单" % cdate, 3)
        order_info = orders.loc[orders['create_time'].str.startswith(cdate)]
        order_info.at[:, 'create_time'] = order_info.loc[:, 'create_time'].str.split().str[1].str[0:8]
        order_info = order_info.reset_index(drop = True)
        t_index = MarkdownTable(headers = ["名称", "方向", "数量", "价格", "创建时间", "完成时间", "对错", "分析"])
        for index in range(len(order_info)):
            data_list = order_info.loc[index].tolist()
            content_list = [data_list[0], data_list[1], int(data_list[2]), round(data_list[3], 2), data_list[4], data_list[5].split(' ')[1].strip()[0:8], '', '']
            content_list = [str(i) for i in content_list]
            t_index.addRow(content_list)
        md.addTable(t_index)

    md.addHeader("本周总结", 2)
    md.addHeader("优点", 3)
    md.addHeader("缺点", 3)
    md.addHeader("心得", 3)
    with open(fullfilepath, "w+") as f:
        f.write(md.getStream())

def main():
    #dirname = '/Volumes/data/quant/stock/data/docs/blog/hellobiek.github.io/source/_posts'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    unlock_path = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/follow_trend.json"
    key_path = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/key.pri"
    futuTrader = FutuTrader(host = ct.FUTU_HOST_LOCAL, port = ct.FUTU_PORT, trd_env = TrdEnv.REAL, market = ct.US_MARKET_SYMBOL, unlock_path = unlock_path, key_path = key_path)
    start = '2020-08-03'
    end   = '2020-08-03'
    orders = futuTrader.get_history_orders(start = start, end = end)
    date_arrary = get_dates_array(start, end, dformat = "%Y-%m-%d", asending = True)
    generate(orders, date_arrary, dirname, start, end)

if __name__ == "__main__": 
    main()
