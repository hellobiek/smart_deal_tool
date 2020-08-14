#-*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
from topten import generate_top10
from hero_list import generate_hero
from daily_collect import generate_daily
from base.cdate import transfer_date_string_to_int

def main():
    mdate = '2020-08-14'
    data_dir = '/Volumes/data/quant/stock/data/crawler/top_list'
    dirname = '/Users/hellobiek/Documents/workspace/blog/blog/source/_posts'
    generate_top10(dirname, mdate)
    generate_daily(dirname, str(transfer_date_string_to_int(mdate)), str(transfer_date_string_to_int(mdate)))
    generate_hero(mdate, data_dir, dirname)

if __name__ == "__main__": 
    main()
