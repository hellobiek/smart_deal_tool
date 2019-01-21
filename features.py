#-*- coding: utf-8 -*-
import array
import numpy as np
import pandas as pd
from pandas import DataFrame
def MA(data, peried):
    return data.rolling(peried).mean()

def SMA(d, N):
    last = np.nan
    v = pd.Series(index=d.index)
    for key in d.index:
        x = d[key]
        x1 = (x + (N - 1) * last) / N if last == last else x
        last = x1
        v[key] = x1
        if x1 != x1: last = x
    return v

def KDJ(data, N1=9, N2=3, N3=3):
    low  = data.low.rolling(N1).min()
    high = data.high.rolling(N1).max()
    rsv  = (data.close - low) / (high - low) * 100
    k = SMA(rsv, N2)
    d = SMA(k, N3)
    j = k * 3 - d * 2
    data['K'] = k
    data['D'] = d
    data['J'] = j
    return data

PRE_DAYS_NUM = 60
DATA_COLUMS = ['date', 'open', 'high', 'close', 'preclose', 'low', 'volume', 'amount', 'outstanding', 'totals', 'adj', 'aprice', 'pchange', 'turnover', 'sai', 'sri', 'uprice', 'ppercent', 'npercent', 'base', 'ibase', 'breakup', 'ibreakup', 'pday', 'profit', 'gamekline']
DTYPE_LIST = [('date', 'S10'),\
              ('open', 'f4'),\
              ('high', 'f4'),\
              ('close', 'f4'),\
              ('preclose', 'f4'),\
              ('low', 'f4'),\
              ('volume', 'i8'),\
              ('amount', 'f4'),\
              ('outstanding', 'i8'),\
              ('totals', 'i8'),\
              ('adj', 'f4'),\
              ('aprice', 'f4'),\
              ('pchange', 'f4'),\
              ('turnover', 'f4'),\
              ('sai', 'f4'),\
              ('sri', 'f4'),\
              ('uprice', 'f4'),\
              ('ppercent', 'f4'),\
              ('npercent', 'f4'),\
              ('base', 'f4'),\
              ('ibase', 'i8'),\
              ('breakup', 'i4'),\
              ('ibreakup', 'i8'),\
              ('pday', 'i4'),\
              ('profit', 'f4'),\
              ('gamekline', 'f4')]

def shift(arr, num, fill_value = 0):
    result = np.empty_like(arr, dtype = float)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result = arr
    return result

def get_effective_breakup_index(break_index_lists, df):
    now_price = 0
    pre_price = 0.0
    break_index = 0
    effective_breakup_index_list = array.array('l', [])
    while break_index < len(break_index_lists):
        pre_price = df['uprice'][break_index_lists[break_index]]
        if break_index < len(break_index_lists) - 1:
            now_price = df['uprice'][break_index_lists[break_index + 1]]
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if break_index_lists[break_index + 1] - break_index_lists[break_index] > PRE_DAYS_NUM:
                    if df['breakup'][break_index_lists[break_index + 1]] * df['breakup'][break_index_lists[break_index]] < 0:
                        effective_breakup_index_list.append(break_index_lists[break_index])
        else:
            now_price = df['uprice'][len(df) - 1]
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if len(df) - break_index_lists[break_index] > PRE_DAYS_NUM:
                    effective_breakup_index_list.append(break_index_lists[break_index])
        break_index += 1
    #ibase means inex of base(effective break up points)
    df['ibase'] = 0
    pre_base_index = 0
    for _index, ibase in enumerate(df['ibase']):
        if _index != 0 and _index in effective_breakup_index_list: pre_base_index = _index
        df['ibase'][_index] = pre_base_index
    return effective_breakup_index_list

def get_breakup_data(df):
    pos_array = np.zeros(len(df), dtype = int)
    pos_array[df.close > df.uprice] = 1
    pos_array[df.close < df.uprice] = -1
    pre_pos_array = shift(pos_array, 1)

    df['breakup'] = 0
    df['breakup'][np.where((pre_pos_array <= 0) & (pos_array > 0))] = 1
    df['breakup'][np.where((pre_pos_array >= 0) & (pos_array < 0))] = -1

    #ibreak up means index of break up
    df['ibreakup'] = 0
    _index = 0
    breakup = 0
    pre_index = 0
    for _index, breakup in enumerate(df['breakup']):
        if _index != 0 and breakup != 0: pre_index = _index
        df['ibreakup'][_index] = pre_index

def base_floating_profit(df, mdate = None):
    s_index = 0
    np_data = df.to_records(index = False)
    np_data = np_data.astype(DTYPE_LIST)
    index_array = np.arange(len(np_data))
    ppchange_array = np.zeros(len(np_data), dtype = float)
    if mdate is None:
        get_breakup_data(np_data)
        break_index_lists = np.where(np_data['breakup'] != 0)[0]
        effective_breakup_index_list = get_effective_breakup_index(break_index_lists, np_data)
        np_data['pday'] = 1
        np_data['base'] = np_data['close'].copy()
        if len(effective_breakup_index_list) == 0:
            np_data['profit'] = (np_data['close'] - np_data['uprice']) / np_data['uprice']
        else:
            for e_index in effective_breakup_index_list:
                if s_index == e_index:
                    if len(effective_breakup_index_list) == 1:
                        base = np_data['uprice'][s_index]
                        direction = np_data['breakup'][s_index]
                        ppchange = 1.1 if direction > 0 else 0.9
                        np_data['base'][s_index] = base
                        ppchange_array[s_index:] = ppchange
                        np_data['pday'][s_index:] = direction * (index_array[s_index:] - s_index + 1)
                else:
                    base = np_data['uprice'][s_index]
                    direction = np_data['breakup'][e_index]
                    ppchange = 1.1 if direction < 0 else 0.9
                    np_data['base'][s_index:e_index] = base
                    ppchange_array[s_index:e_index] = ppchange
                    np_data['pday'][s_index:e_index] = -1 * direction * (index_array[s_index:e_index] - s_index + 1)
                    s_index = e_index
                    if e_index == effective_breakup_index_list[-1]:
                        base = np_data['uprice'][e_index]
                        direction = np_data['breakup'][e_index]
                        ppchange = 1.1 if direction > 0 else 0.9
                        np_data['base'][e_index:] = base
                        ppchange_array[e_index:] = ppchange
                        np_data['pday'][e_index:] = direction * (index_array[e_index:] - e_index + 1)
            np_data['profit'] = abs(np.log(np_data['close']) - np.log(np_data['base'])) / np.log(ppchange_array)
    df = DataFrame(data = np_data, columns = DATA_COLUMS)
    df.date = df.date.str.decode('utf-8')
    return df
