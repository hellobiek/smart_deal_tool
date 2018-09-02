# coding:utf-8
import time
import _pickle
import const as ct
import numpy as np
import pandas as pd
from datetime import datetime
from common import get_market_name, create_redis_obj, delta_days

def nqfq(data, code, info):
    if 0 == len(info): return data
    redis = create_redis_obj()
    df_byte = redis.get(ct.STOCK_INFO)
    df = _pickle.loads(df_byte)
    startdate = df.loc[df.code == code]['timeToMarket'].values[0]

    startdate = datetime.strptime(str(startdate), "%Y%m%d").strftime('%Y-%m-%d')
    enddate = datetime.now().strftime('%Y-%m-%d')
    num_days = delta_days(startdate, enddate)
    start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(startdate, "%Y-%m-%d"))
    data_times = pd.date_range(start_date_dmy_format, periods = num_days, freq='D')
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(data_times.to_pydatetime())

    data_dict = dict()
    data_size = date_only_array.size
    data_dict['date'] = date_only_array.tolist()
    data_dict['adj'] = [1 for x in range(data_size)]
    df = pd.DataFrame.from_dict(data_dict)

def qfq(data, code, info):
    data['preclose'] = data['close'].shift(-1)
    if 0 == len(info): return data
    for info_index, start_date in info.date.iteritems():
        dates = data.loc[data.date <= start_date].index.tolist()
        if 0 == len(dates): continue
        start_index = dates[0]
        rate  = info.loc[info_index, 'rate']    #配k股
        price = info.loc[info_index, 'price']   #配股价格
        money = info.loc[info_index, 'money']   #分红
        count = info.loc[info_index, 'count']   #转送股数量
        adj = (data.loc[start_index, 'preclose'] * 10 - money + rate) / ((10 + rate + count) * data.loc[start_index, 'preclose'])
        #adjust price
        data.at[start_index + 1:, 'low']      = data.loc[start_index + 1:, 'low'] * adj
        data.at[start_index + 1:, 'open']     = data.loc[start_index + 1:, 'open'] * adj
        data.at[start_index + 1:, 'high']     = data.loc[start_index + 1:, 'high'] * adj
        data.at[start_index + 1:, 'close']    = data.loc[start_index + 1:, 'close'] * adj
        data.at[start_index + 1:, 'volume']   = data.loc[start_index + 1:, 'volume'] / adj
        data.at[start_index + 1:, 'preclose'] = data.loc[start_index + 1:, 'preclose'] * adj
    return data

if __name__ == '__main__':
    code = '601318'
    prestr = "1" if get_market_name(code) == "sh" else "0"
    filename = "%s%s.csv" % (prestr, code)
    data = pd.read_csv("/data/tdx/history/days/%s" % filename, sep = ',')
    data = data[['date', 'open', 'low', 'high', 'close', 'volume', 'amount']]
    data = data.sort_index(ascending = False)
    data = data.reset_index(drop = True)

    info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    info = info[(info.code == code) & (info.type == 1)]
    info = info.sort_index(ascending = False)
    info = info.reset_index(drop = True)
    info = info[['money', 'price', 'count', 'rate', 'date']]
    data = nqfq(data, code, info)
