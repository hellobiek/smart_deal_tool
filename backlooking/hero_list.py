#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from tools.markdown_table import MarkdownTable
from tools.markdown_writer import MarkdownWriter
def get_hero_list_info(mdate, data_dir):
    filename = '{}.csv'.format(mdate)
    fullfilepath = os.path.join(data_dir, filename)
    df = pd.read_csv(fullfilepath, sep = ',', dtype = {'date':str, 'code':str, 'name': str, 'type': str, 'buy': int, 'sell': int})
    df = df.drop_duplicates()
    df = df.reset_index(drop = True)
    df['net'] = df['buy'] - df['sell']
    df_one = df.loc[df.type == '1日']
    df_three = df.loc[df.type == '3日']
    df_one = df_one.sort_values(by = 'net', ascending = False)
    df_one = df_one.reset_index(drop = True)
    df_three = df_three.sort_values(by = 'net', ascending = False)
    df_three = df_three.reset_index(drop = True)
    return df_one, df_three

def generate_hero(mdate, data_dir, dirname):
    filename = 'hero_list_{}.md'.format(mdate)
    fullfilepath = os.path.join(dirname, filename)
    hero_info_one, hero_info_three = get_hero_list_info(mdate, data_dir)
    md = MarkdownWriter()
    md.addHeader("{}龙虎榜".format(mdate), 1)
    t_index = MarkdownTable(headers = ["代码", "名称", "类型", "买入额", "卖出额", "净额"])
    for index in range(len(hero_info_one)):
        data_list =  hero_info_one.loc[index].tolist()
        content_list = [data_list[1], data_list[2], data_list[3], data_list[4], data_list[5], data_list[6]]
        content_list = [str(i) for i in content_list]
        t_index.addRow(content_list)

    for index in range(len(hero_info_three)):
        data_list =  hero_info_three.loc[index].tolist()
        content_list = [data_list[1], data_list[2], data_list[3], data_list[4], data_list[5], data_list[6]]
        content_list = [str(i) for i in content_list]
        t_index.addRow(content_list)
    md.addTable(t_index)
    with open(fullfilepath, "w+") as f:
        f.write(md.getStream())

def main():
    mdate = '2020-07-29'
    data_dir = '/Volumes/data/quant/stock/data/crawler/top_list'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    generate_hero(mdate, data_dir, dirname)

if __name__ == "__main__": 
    main()
