#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from cstock_info import CStockInfo
from common import get_tushare_client
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
from base.cdate import transfer_int_to_date_string
dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
def get_up_data(mdate):
    fpath = '/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/tushare.json'
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1', stocks_dir = stocks_dir, base_stock_path = base_stock_path)
    base_df = stock_info_client.get()
    tu_client = get_tushare_client(fpath)
    df = tu_client.daily(trade_date=mdate)
    df = df.loc[df.pct_chg > 9.5]
    df = df.reset_index(drop = True)
    df['ts_code'] = df['ts_code'].str[0:6]
    df['trade_date'] = transfer_int_to_date_string(mdate)
    df = df.rename(columns = {"ts_code": "code", "trade_date": "date"})
    df = df[['date', 'code']]
    df = pd.merge(df, base_df, how='inner', on=['code'])
    df = df[['date', 'code', 'name']]
    return df

def generate(dirname, mdate):
    filename = 'daily_up_{}.md'.format(mdate)
    fullfilepath = os.path.join(dirname, filename)
    info = get_up_data(mdate)
    md = MarkdownWriter()
    md.addHeader("{}交割单分析".format(mdate), 1)
    t_index = MarkdownTable(headers = ["日期", "代码", "名称", "概念", "分析"])
    for index in range(len(info)):
        data_list = info.loc[index].tolist()
        content_list = [data_list[0], data_list[1], data_list[2], '    ', '']
        content_list = [str(i) for i in content_list]
        t_index.addRow(content_list)
    md.addTable(t_index)
    with open(fullfilepath, "w+") as f:
        f.write(md.getStream())

def main():
    mdate = '20200708'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    generate(dirname, mdate)

if __name__ == "__main__": 
    main()
