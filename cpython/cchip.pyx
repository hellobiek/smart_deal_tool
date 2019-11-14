# -*- coding: utf-8 -*-
# cython: language_level=3, wraparound=False, boundscheck=False, nonecheck=False, infer_types=True
import array
import numpy as np
import pandas as pd
from cpython cimport array
cimport numpy as np
from pandas import DataFrame
CHIP_COLUMNS = ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
DTYPE_LIST = [('pos', 'i8'), ('sdate', 'S10'), ('date', 'S10'), ('price', 'f4'), ('volume', 'i8'), ('outstanding', 'i8')]
def evenly_distributed_chip(np.ndarray[long] volume_series, long pre_outstanding, long outstanding):
    volume_series = (outstanding * (volume_series / pre_outstanding)).astype(long)
    cdef long delta = 0
    cdef long real_total_volume = np.sum(volume_series)
    cdef long delta_sum = outstanding - real_total_volume
    while abs(delta_sum) >= len(volume_series):
        volume_series += long(delta_sum / len(volume_series))
        delta_sum = outstanding - np.sum(volume_series)
        if delta_sum == 0: return volume_series
    delta = 1 if delta_sum > 0 else -1
    volume_series[np.argpartition(volume_series, abs(delta_sum))[:abs(delta_sum)]] += delta
    return volume_series

def max_min_normalization(np.ndarray[float] series):
    cdef float max_val = max(series)
    cdef float min_val = min(series)
    if abs(max_val - min_val) < 0.0001:
        return (np.asarray([1/len(series)for i in range(len(series))])).astype(np.float32)
    else:
        return ((series - np.mean(series))/(max_val - min_val)).astype(np.float32)

def postive_normalization(np.ndarray[float] series):
    cdef float max_val = max(series)
    cdef float min_val = min(series)
    cdef np.ndarray[float] nseries
    if abs(max_val - min_val) < 0.0001:
        return np.asarray([1/len(series)for i in range(len(series))]).astype(np.float32)
    else:
        nseries = series + 2 * abs(max(series))
        return (nseries/np.sum(nseries)).astype(np.float32)

def allocate_volume(long volume, np.ndarray[float] ratio_series, np.ndarray[long] volume_series):
    cdef float ratio = 0.0
    cdef long index = 0, expected_volume = 0
    while volume != 0:
        for (index, ), ratio in np.ndenumerate(ratio_series):
            expected_volume = min(max(1, int(volume * ratio)), volume)
            if volume >= expected_volume:
                if volume_series[index] >= expected_volume:
                    volume_series[index] -= expected_volume
                    volume -= expected_volume
                else:
                    volume -= volume_series[index]
                    volume_series[index] = 0
            else:
                if volume_series[index] >= volume:
                    volume_series[index] -= volume
                    volume = 0
                else:
                    volume -= volume_series[index]
                    volume_series[index] = 0
            if 0 == volume: break
    return volume_series

def divide_according_price(np.ndarray[float] price_series, np.ndarray[long] volume_series, long volume, float price):
    cdef np.ndarray[float] delta_price_series = max_min_normalization(price - price_series)
    cdef np.ndarray[float] volume_normalization_series = (volume_series / max(volume_series)).astype(np.float32)
    cdef np.ndarray[float] delta_volume_series = max_min_normalization(volume_normalization_series)
    cdef np.ndarray[float] ratio_series = postive_normalization(0.1 * delta_price_series + 0.9 * delta_volume_series)
    return allocate_volume(volume, ratio_series, volume_series)

def divide_according_position(np.ndarray[long] position_series, np.ndarray[long] volume_series, long volume, long position):
    cdef np.ndarray[float] delta_position_series = max_min_normalization((position - position_series).astype(np.float32))
    cdef np.ndarray[float] volume_normalization_series = (volume_series / max(volume_series)).astype(np.float32)
    cdef np.ndarray[float] delta_volume_series = max_min_normalization(volume_normalization_series)
    cdef np.ndarray[float] ratio_series = postive_normalization(0.1 * delta_position_series + 0.9 * delta_volume_series)
    return allocate_volume(volume, ratio_series, volume_series)

def number_of_days(np.ndarray[long] pre_pos, long pos):
    return pos - pre_pos

def divide_data(np.ndarray mdata, long pos, float price):
    cdef np.ndarray s_data, s_p_data, s_u_data, l_data, l_p_data, l_u_data
    #short chip data
    s_data = mdata[np.apply_along_axis(number_of_days, 0, mdata['pos'], pos) <= 60]
    #short profit data
    s_p_data = s_data[s_data['price'] <= price]
    #short unprofit data
    s_u_data = s_data[s_data['price'] > price]
    #long chip data
    l_data = mdata[np.apply_along_axis(number_of_days, 0, mdata['pos'], pos) > 60]
    #long profit data
    l_p_data = l_data[l_data['price'] <= price]
    #long unprofit data
    l_u_data = l_data[l_data['price'] > price]
    return s_p_data, s_u_data, l_p_data, l_u_data

def divide_volume(long volume, long s_p_volume_total, long s_u_volume_total, long l_p_volume_total, long l_u_volume_total, long volume_total):
    cdef long s_p_volume = 0, s_u_volume = 0, l_p_volume = 0, l_u_volume = 0
    cdef long max_volume = max(s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total)
    if s_p_volume_total == max_volume:
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        l_u_volume = long(volume * (l_u_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        s_p_volume = volume - s_u_volume - l_p_volume - l_u_volume
    elif s_u_volume_total == max_volume:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        l_u_volume = long(volume * (l_u_volume_total / volume_total))
        s_u_volume = volume - s_p_volume - l_p_volume - l_u_volume
    elif l_p_volume_total == max_volume:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        l_u_volume = long(volume * (l_u_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_p_volume = volume - s_p_volume - s_u_volume - l_u_volume
    else:
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_u_volume = volume - s_p_volume - s_u_volume - l_p_volume
    return s_p_volume, s_u_volume, l_p_volume, l_u_volume

def adjust_volume(np.ndarray mdata, long pos, long volume, float price, long pre_outstanding, long outstanding):
    if pre_outstanding != outstanding:
        mdata['volume'] = evenly_distributed_chip(mdata['volume'], pre_outstanding, outstanding)

    cdef np.ndarray s_p_data, s_u_data, l_p_data, l_u_data
    s_p_data, s_u_data, l_p_data, l_u_data = divide_data(mdata, pos, price)

    #total volume
    cdef long volume_total = outstanding
    cdef long s_p_volume_total = np.sum(s_p_data['volume'])
    cdef long s_u_volume_total = np.sum(s_u_data['volume'])
    cdef long l_p_volume_total = np.sum(l_p_data['volume'])
    cdef long l_u_volume_total = np.sum(l_u_data['volume'])

    cdef long s_p_volume = 0, s_u_volume = 0, l_p_volume = 0, l_u_volume = 0
    s_p_volume, s_u_volume, l_p_volume, l_u_volume = divide_volume(volume, s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total, volume_total)

    if s_p_volume > 0:s_p_data['volume'] = divide_according_price(s_p_data['price'], s_p_data['volume'], s_p_volume, price)
    if s_u_volume > 0:s_u_data['volume'] = divide_according_position(s_u_data['pos'], s_u_data['volume'], s_u_volume, pos)
    if l_p_volume > 0:l_p_data['volume'] = divide_according_price(l_p_data['price'], l_p_data['volume'], l_p_volume, price)
    if l_u_volume > 0:l_u_data['volume'] = divide_according_position(l_u_data['pos'], l_u_data['volume'], l_u_volume, pos)
    return np.concatenate((s_p_data, s_u_data, l_p_data, l_u_data), axis = 0)

def compute_oneday_distribution(pre_date_dist, char *cdate, long pos, long volume, float aprice, long pre_outstanding, long outstanding):
    cdef np.ndarray np_pre_data = pre_date_dist.to_records(index = False)
    np_pre_data = np_pre_data.astype(DTYPE_LIST)
    np_pre_data = adjust_volume(np_pre_data, pos, volume, aprice, pre_outstanding, outstanding)
    np_pre_data['date'] = cdate
    np_pre_data['outstanding'] = outstanding
    np_pre_data = np.concatenate((np_pre_data, np.array([(pos, cdate, cdate, aprice, volume, outstanding)], dtype = DTYPE_LIST)), axis=0)
    df = DataFrame(data = np_pre_data, columns = CHIP_COLUMNS)
    df = df[df.volume != 0]
    df.date = df.date.str.decode('utf-8')
    df.sdate = df.sdate.str.decode('utf-8')
    df.price = df.price.astype(float).round(2)
    return df.reset_index(drop = True)

def compute_distribution(data):
    cdef char *cdate
    cdef float aprice, open_price = data.at[0, 'open']
    cdef long pos, volume, index, outstanding, pre_outstanding = 0
    data = data[['date', 'volume', 'aprice', 'outstanding']]
    with pd.option_context('mode.chained_assignment', None):
        data['date'] = data['date'].str.encode("UTF-8")
    cdef np.ndarray np_data = data.values
    tmp_arrary = np.zeros((2, 6), dtype = DTYPE_LIST)
    data_arrary = np.zeros((2, 6), dtype = DTYPE_LIST)
    for index, row in enumerate(np_data):
        cdate, volume, aprice, outstanding = row[[0, 1, 2, 3]]
        if 0 == index:
            t1 = (index, cdate, cdate, aprice, volume, outstanding)
            t2 = (index, cdate, cdate, open_price, outstanding - volume, outstanding)
            t = np.array([t1, t2], dtype = DTYPE_LIST)
            tmp_arrary = t.copy()
        else:
            tmp_arrary = adjust_volume(tmp_arrary, index, volume, aprice, pre_outstanding, outstanding)
            tmp_arrary['date'] = cdate
            tmp_arrary['outstanding'] = outstanding
            tdata = (index, cdate, cdate, aprice, volume, outstanding)
            t = np.array([tdata], dtype = DTYPE_LIST)
            tmp_arrary = np.concatenate((tmp_arrary, np.array(t)), axis=0)
        pre_outstanding = outstanding
        tmp_arrary = tmp_arrary[tmp_arrary['volume'] > 0]
        data_arrary = tmp_arrary.copy() if 0 == index else np.concatenate((data_arrary, tmp_arrary), axis = 0)
    df = DataFrame(data = data_arrary, columns = CHIP_COLUMNS)
    with pd.option_context('mode.chained_assignment', None):
        df['date'] = df['date'].str.decode('utf-8')
        df['sdate'] = df['sdate'].str.decode('utf-8')
        df['price'] = df['price'].astype(float).round(2)
    return df

def mac(data, int peried):
    ulist = list()
    for name, group in data.groupby(data.date):
        if peried != 0: group = group.nlargest(peried, 'pos')
        total_volume = group.volume.sum()
        total_amount = group.price.dot(group.volume)
        ulist.append(total_amount / total_volume)
    return ulist

def mac1(data, perieds):
    cdef int peried
    cdef str ndate
    cdef char* cdate
    cdef long total_volume = 0
    cdef float total_amount = 0
    cdef array.array ulist = array.array('f', [])
    cdef array.array slist = array.array('f', [])
    cdef array.array mlist = array.array('f', [])
    cdef array.array llist = array.array('f', [])
    cdef np.ndarray group, dist_data = data.to_records(index = False).astype(DTYPE_LIST)
    for peried in perieds:
        for cdate in np.unique(dist_data['date']):
            ndate = cdate.decode('utf-8')
            group = dist_data[np.where(np.char.decode(dist_data['date']) == ndate)]
            if peried != 0 and len(group) > peried:
                group = group[np.argpartition(group['pos'], -peried)][-peried:]
            total_volume = group['volume'].sum()
            total_amount = group['price'].dot(group['volume'])
            if peried == 0:
                ulist.append(total_amount / total_volume)
            elif peried == 5:
                slist.append(total_amount / total_volume)
            elif peried == 13:
                mlist.append(total_amount / total_volume)
            else:
                llist.append(total_amount / total_volume)
    return ulist, slist, mlist, llist

def divide_according_price1(np.ndarray[float] price_series, np.ndarray[long] volume_series, long total_volume, float price):
    cdef np.ndarray[float] delta_price_series = np.abs(price - price_series)
    cdef float total_delta_price = np.sum(delta_price_series)
    cdef float ratio = 0
    cdef long tmp_volume = 0
    cdef long length = len(price_series)
    while total_volume != 0:
        tmp_volume = total_volume
        for (index, ), delta_price in np.ndenumerate(delta_price_series):
            ratio = tmp_volume / length if 0 == total_delta_price else delta_price / total_delta_price
            expected_volume = max(1, int(tmp_volume * ratio))
            if expected_volume > total_volume: expected_volume = total_volume
            total_volume -= min(volume_series[index], expected_volume)
            volume_series[index] = max(0, volume_series[index] - expected_volume)
            if 0 == total_volume: break
    return volume_series
