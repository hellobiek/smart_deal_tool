#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from common import str_of_num
from visualization.dash.hgt import HGT
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
pd.options.mode.chained_assignment = None
def get_top20_stock_info_from_hgt(cdate):
    hgt_client = HGT(dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    info = hgt_client.get_top10_info(cdate)
    info['net_turnover'] = info['buy_turnover'] - info['sell_turnover']
    info = info.sort_values(by = 'net_turnover', ascending= False)
    info = info.drop('rank', axis = 1)
    info = info.reset_index(drop = True)
    info['total_turnover'] = info['total_turnover'].apply(lambda x:str_of_num(x))
    info['net_turnover'] = info['net_turnover'].apply(lambda x:str_of_num(x))
    info['buy_turnover'] = info['buy_turnover'].apply(lambda x:str_of_num(x))
    info['sell_turnover'] = info['sell_turnover'].apply(lambda x:str_of_num(x))
    return info

def generate_top10(dirname, mdate):
    filename = 'topten_review_{}.md'.format(mdate)
    fullfilepath = os.path.join(dirname, filename)
    top20_info = get_top20_stock_info_from_hgt(mdate)
    md = MarkdownWriter()
    md.addHeader("{}交割单分析".format(mdate), 1)
    t_index = MarkdownTable(headers = ["代码", "名称", "总成交额", "买入额", "卖出额", "净买入额"])
    for index in range(len(top20_info)):
        data_list = top20_info.loc[index].tolist()
        content_list = [data_list[1], data_list[2], data_list[3], data_list[4], data_list[5], data_list[6]]
        content_list = [str(i) for i in content_list]
        t_index.addRow(content_list)
    md.addTable(t_index)
    with open(fullfilepath, "w+") as f:
        f.write(md.getStream())

def main():
    mdate = '2020-07-29'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    generate(dirname, mdate)

if __name__ == "__main__": 
    main()
