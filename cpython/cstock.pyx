# cython: language_level=3, boundscheck=False, nonecheck=False, infer_types=True
import array
import numpy as np
cimport numpy as np
from cpython cimport array
from pandas import DataFrame
PRE_DAYS_NUM = 60
DATA_COLUMS = ['date', 'open', 'high', 'close', 'preclose', 'low', 'volume', 'amount', 'outstanding', 'totals', 'adj', 'aprice', 'pchange', 'turnover', 'sai', 'sri', 'uprice', 'sprice', 'mprice', 'lprice', 'ppercent', 'npercent', 'base', 'ibase', 'breakup', 'ibreakup', 'pday', 'profit', 'gamekline']
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
              ('sprice', 'f4'),\
              ('mprice', 'f4'),\
              ('lprice', 'f4'),\
              ('ppercent', 'f4'),\
              ('npercent', 'f4'),\
              ('base', 'f4'),\
              ('ibase', 'i8'),\
              ('breakup', 'i4'),\
              ('ibreakup', 'i8'),\
              ('pday', 'i4'),\
              ('profit', 'f4'),\
              ('gamekline', 'f4')]

def cyappend(array.array arr, long x):
    cdef array.array res = array.copy(arr)
    res.append(x)
    return res

def shift(np.ndarray[long] arr, int num, fill_value = 0):
    cdef np.ndarray result = np.empty_like(arr, dtype = long)
    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result = arr
    return result

def get_effective_breakup_index(np.ndarray[long] break_index_lists, np.ndarray df):
    cdef float pre_price = 0.0
    cdef long break_index = 0, now_price = 0
    cdef array.array effective_breakup_index_list = array.array('l', [])
    while break_index < len(break_index_lists):
        pre_price = df['uprice'][break_index_lists[break_index]]
        if break_index < len(break_index_lists) - 1:
            now_price = df['uprice'][break_index_lists[break_index + 1]]
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list = cyappend(effective_breakup_index_list, break_index_lists[break_index])
            else:
                if break_index_lists[break_index + 1] - break_index_lists[break_index] > PRE_DAYS_NUM:
                    if df['breakup'][break_index_lists[break_index + 1]] * df['breakup'][break_index_lists[break_index]] > 0: raise("get error ip failed")
                    effective_breakup_index_list = cyappend(effective_breakup_index_list, break_index_lists[break_index])
        else:
            now_price = df['uprice'][len(df) - 1]
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list = cyappend(effective_breakup_index_list, break_index_lists[break_index])
            else:
                if len(df) - break_index_lists[break_index] > PRE_DAYS_NUM:
                    effective_breakup_index_list = cyappend(effective_breakup_index_list, break_index_lists[break_index])
        break_index += 1
    #ibase means inex of base(effective break up points)
    df['ibase'] = 0
    cdef long pre_base_index = 0
    for _index, ibase in enumerate(df['ibase']):
        if _index != 0 and _index in effective_breakup_index_list: pre_base_index = _index
        df['ibase'][_index] = pre_base_index
    return effective_breakup_index_list

def get_breakup_data(np.ndarray df):
    cdef np.ndarray pos_array = np.zeros(len(df), dtype = int)
    pos_array[df.close > df.uprice] = 1
    pos_array[df.close < df.uprice] = -1
    cdef np.ndarray pre_pos_array = shift(pos_array, 1)

    df['breakup'] = 0
    df['breakup'][np.where((pre_pos_array <= 0) & (pos_array > 0))] = 1
    df['breakup'][np.where((pre_pos_array >= 0) & (pos_array < 0))] = -1

    #ibreak up means index of break up
    df['ibreakup'] = 0
    cdef int breakup = 0
    cdef long pre_index = 0, _index = 0
    for _index, breakup in enumerate(df['breakup']):
        if _index != 0 and breakup != 0: pre_index = _index
        df['ibreakup'][_index] = pre_index

def base_floating_profit(df, mdate = None):
    cdef int direction = 0
    cdef float base, ppchange
    cdef long s_index = 0, e_index = 0
    cdef np.ndarray np_data = df.to_records(index = False).astype(DTYPE_LIST, copy = False)
    cdef np.ndarray break_index_lists
    cdef array.array effective_breakup_index_list
    cdef np.ndarray index_array = np.arange(len(np_data))
    cdef np.ndarray ppchange_array = np.zeros(len(np_data), dtype = float)
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

def pro_nei_chip(df, dist_data, preday_df = None, mdate = None):
    if mdate is None:
        p_profit_vol_list = list()
        p_neighbor_vol_list = list()
        groups = dist_data.groupby(dist_data.date)
        for _index, cdate in df.date.iteritems():
            drow = df.loc[_index]
            close_price = drow['close']
            outstanding = drow['outstanding']
            group = groups.get_group(cdate)
            p_val = 100 * group[group.price < close_price].volume.sum() / outstanding
            n_val = 100 * group[(group.price < close_price * 1.075) & (group.price > close_price * 0.925)].volume.sum() / outstanding
            p_profit_vol_list.append(p_val)
            p_neighbor_vol_list.append(n_val)
        df['ppercent'] = p_profit_vol_list
        df['npercent'] = p_neighbor_vol_list
        df['gamekline'] = df['ppercent'] - df['ppercent'].shift(1)
        df.at[0, 'gamekline'] = df.loc[0, 'ppercent']
    else:
        p_close     = df['close'].values[0]
        outstanding = df['outstanding'].values[0]
        p_val = 100 * dist_data[dist_data.price < p_close].volume.sum() / outstanding
        n_val = 100 * dist_data[(dist_data.price < p_close * 1.08) & (dist_data.price > p_close * 0.92)].volume.sum() / outstanding
        df['ppercent'] = p_val
        df['npercent'] = n_val
        df['gamekline'] = df['ppercent'].values[0] - preday_df['ppercent'].values[0]
    return df
