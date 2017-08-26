# -*- coding:utf-8 -*-
import sys
import numpy as np
import tushare as ts
from mysql import get_hist_data
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from const import MARKET_SH,MARKET_SZ,MARKET_CYB,MARKET_ALL,DB_NAME,DB_USER,DB_PASSWD,DB_HOSTNAME,SQL

engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
code_id = '002460'

hist_data = ts.get_h_data(code_id, start = '2016-01-01', end = '2017-07-21')
hist_data = hist_data.reset_index(drop = False)

dates = hist_data['date'].tolist()
dates.reverse()

open_vals = hist_data['open'].tolist()
open_vals.reverse()

close_vals = hist_data['close'].tolist()
close_vals.reverse()

length = hist_data.shape[0]
i = 0
_index = 0
_indexs = list()
while i < length:
    open_price = open_vals[_index]
    if i + 5 < length:
        _index += 5
    else:
        _index = length - 1
    close_price = close_vals[_index]
    p_change = 100 * (close_price - open_price) / open_price
    if p_change > 5 or p_change < -5:
        _indexs.append(_index)
    i += 5

x = np.arange(length)
y = np.array(close_vals)

plot = plt.plot(x, y, '*',label='original values')
for _index in _indexs:
    plt.plot(_index, close_vals[_index], 'gs')

plt.xlabel('x axis')
plt.ylabel('y axis')
plt.legend(loc=4)
plt.title('polyfitting')
plt.show()
