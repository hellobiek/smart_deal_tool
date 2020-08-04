#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from cstock_info import CStockInfo
from cpython.cval import CValuation
from common import get_tushare_client
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
from base.cdate import transfer_int_to_date_string
dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
def get_up_data(mdate):
    fpath = '/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/tushare.json'
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1', stocks_dir = stocks_dir, base_stock_path = base_stock_path)
    base_df = stock_info_client.get()
    tu_client = get_tushare_client(fpath)
    val_client = CValuation(valuation_path, bonus_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir)
    df = tu_client.daily(trade_date=mdate)
    #df['amount'] = df['amount'] / 100000 
    df = df.loc[df.pct_chg > 9.5]
    df = df.reset_index(drop = True)
    df['ts_code'] = df['ts_code'].str[0:6]
    df['trade_date'] = transfer_int_to_date_string(mdate)
    df = df.rename(columns = {"ts_code": "code", "trade_date": "date"})
    df = df[['date', 'code']]
    df = pd.merge(df, base_df, how='inner', on=['code'])
    df = df.loc[df.timeToMarket - int(mdate) < -100]
    df = df[['date', 'code', 'name', 'industry', 'timeToMarket']]
    val_client.update_vertical_data(df, ['fund_holders'], int(mdate))
    df = df[['date', 'code', 'name', 'industry', 'fund_holders']]
    df = df.sort_values(by = 'industry', ascending= True)
    df = df.reset_index(drop = True)
    return df

def generate_daily(dirname, mdate):
    info = get_up_data(mdate)
    if info.empty:
        print("{} data is empty".format(mdate))
        return 
    filename = 'daily_up_{}.md'.format(mdate)
    fullfilepath = os.path.join(dirname, filename)
    md = MarkdownWriter()
    md.addHeader("{}交割单分析".format(mdate), 1)
    t_index = MarkdownTable(headers = ["日期", "代码", "名称", "概念", "机构关注度", "分析"])
    for index in range(len(info)):
        data_list = info.loc[index].tolist()
        content_list = [data_list[0], data_list[1], data_list[2], data_list[3], int(data_list[4]), '']
        content_list = [str(i) for i in content_list]
        t_index.addRow(content_list)
    md.addTable(t_index)
    with open(fullfilepath, "w+") as f:
        f.write(md.getStream())

def main():
    mdate = '20200803'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    generate_daily(dirname, mdate)

if __name__ == "__main__": 
    main()
