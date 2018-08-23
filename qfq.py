# coding:utf-8
import pandas as pd
from common import get_market_name
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
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
        data.at[start_index + 1:, 'open']     = data.loc[start_index + 1:, 'open'] * adj
        data.at[start_index + 1:, 'high']     = data.loc[start_index + 1:, 'high'] * adj
        data.at[start_index + 1:, 'low']      = data.loc[start_index + 1:, 'low'] * adj
        data.at[start_index + 1:, 'close']    = data.loc[start_index + 1:, 'close'] * adj
        data.at[start_index + 1:, 'preclose'] = data.loc[start_index + 1:, 'preclose'] * adj
        data.at[start_index + 1:, 'volume']   = data.loc[start_index + 1:, 'volume'] / adj
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

    data = qfq(data, code, info)
