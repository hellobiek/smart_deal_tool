# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from cindex import CIndex
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
code = '399006'
start_date = '2019-01-01'
end_date = '2019-08-11'
df = CIndex(code).get_val_data()
df = df.sort_values(by=['date'], ascending = True)
df = df.reset_index(drop = True)
print(df.loc[(df.date > start_date) & (df.date < end_date)])
