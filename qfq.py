# coding:utf-8
import time
import const as ct
import pandas as pd
from log import getLogger
from datetime import datetime
from common import get_market_name

pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

logger = getLogger(__name__)

def qfq(data, code, info):
    if 0 == len(info): return data
    for info_index, start_date in info.date.iteritems():
        dates = data.loc[data.date >= start_date].index.tolist()
        if len(dates) == 0 : continue
        rate  = info.loc[info_index, 'rate']    #配k股
        price = info.loc[info_index, 'price']   #配股价格
        money = info.loc[info_index, 'money']   #分红
        count = info.loc[info_index, 'count']   #转送股数量
        start_index = dates[len(dates) - 1]
        adj = (data.loc[start_index, 'preclose'] * 10 - money + rate * price) / ((10 + rate + count) * data.loc[start_index, 'preclose'])
        data.at[start_index + 1:, 'adj'] = data.loc[start_index + 1:, 'adj'] * adj
    return data

def adjust_share(data, code, info):
    if 0 == len(info): return data
    end_index = 0
    pre_totals = 0
    pre_outstanding = 0
    last_pre_totals = 0
    last_pre_outstanding = 0
    for info_index, start_date in info.date.iteritems():
        dates = data.loc[data.date >= start_date].index.tolist()
        if len(dates) == 0 : continue
        start_index = end_index
        end_index = dates[len(dates) - 1]

        pre_outstanding = int(info.loc[info_index, 'money'])   #前流通盘
        pre_totals = int(info.loc[info_index, 'price'])   #前总股本
        cur_outstanding = int(info.loc[info_index, 'count'])   #后流通盘
        cur_totals = int(info.loc[info_index, 'rate'])    #后总股本

        if 0 == info_index:
            data.at[start_index:end_index, 'outstanding'] = cur_outstanding
            data.at[start_index:end_index, 'totals'] = cur_totals
            last_pre_outstanding = pre_outstanding
            last_pre_totals = pre_totals
        else:
            if cur_outstanding != last_pre_outstanding:
                logger.debug("%s 日期:%s 前流通盘:%s 不等于 预期前流通盘:%s" % (code, start_date, cur_outstanding, last_pre_outstanding))
            elif cur_totals != last_pre_totals:
                logger.debug("%s 日期:%s 后流通盘:%s 不等于 预期后流通盘:%s" % (code, start_date, cur_totals, last_pre_totals))
            data.at[start_index + 1:end_index, 'outstanding'] = cur_outstanding
            data.at[start_index + 1:end_index, 'totals'] = cur_totals
            last_pre_outstanding = pre_outstanding
            last_pre_totals = pre_totals

            #finish the last date
            if info_index == len(info) - 1:
                data.at[end_index + 1:, 'outstanding'] = last_pre_outstanding
                data.at[end_index + 1:, 'totals'] = last_pre_totals
    return data

if __name__ == '__main__':
    code = '000677'
    prestr = "1" if get_market_name(code) == "sh" else "0"
    filename = "%s%s.csv" % (prestr, code)
    data = pd.read_csv("/data/tdx/history/days/%s" % filename, sep = ',')
    data = data[['date', 'open', 'low', 'high', 'close', 'volume', 'amount']]
    data = data.sort_index(ascending = False)
    data = data.reset_index(drop = True)

    info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    info = info[(info.code == code) & (info.date <= int(datetime.now().strftime('%Y%m%d')))]
    info = info.sort_index(ascending = False)
    info = info.reset_index(drop = True)

    total_stock_change_type_list = ['2', '3', '4', '5', '7', '8', '9', '10', '11']
    s_info = info[info.type.isin(total_stock_change_type_list)]
    s_info = s_info[['date', 'type', 'money', 'price', 'count', 'rate']] 
    s_info = s_info.sort_index(ascending = True)
    s_info = s_info.reset_index(drop = True)

    data['outstanding'] = 0
    data['totals'] = 0
    data = adjust_share(data, code, s_info)

    data['preclose'] = data['close'].shift(-1)
    data['adj'] = 1.0

    t_info = info[info.type == 1]
    t_info = t_info[['money', 'price', 'count', 'rate', 'date']]
    t_info = t_info.sort_index(ascending = True)
    t_info = t_info.reset_index(drop = True)

    data = qfq(data, code, t_info)
    data = data[['date', 'open', 'high', 'close', 'pre_close', 'low', 'volume', 'amount', 'outstanding', 'totals', 'adj']]
    #print(data)
