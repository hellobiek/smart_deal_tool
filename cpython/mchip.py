# cython: language_level=3, boundscheck=False, nonecheck=False, infer_types=True
import time
import numpy as np
import pandas as pd
from pandas import DataFrame
CHIP_COLUMNS = ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
DTYPE_LIST = [('pos', 'i8'), ('sdate', 'S10'), ('date', 'S10'), ('price', 'f4'), ('volume', 'i8'), ('outstanding', 'i8')]
def evenly_distributed_chip(volume_series, pre_outstanding, outstanding):
    volume_series = (outstanding * (volume_series / pre_outstanding)).astype(int)
    real_total_volume = np.sum(volume_series)
    delta_sum = outstanding - real_total_volume
    while abs(delta_sum) >= len(volume_series):
        volume_series += int(delta_sum / len(volume_series))
        delta_sum = outstanding - np.sum(volume_series)
        if delta_sum == 0: return volume_series
    delta = 1 if delta_sum > 0 else -1
    volume_series[np.argpartition(volume_series, abs(delta_sum))[:abs(delta_sum)]] += delta
    return volume_series

def max_min_normalization(series):
    max_val = max(series)
    min_val = min(series)
    if abs(max_val - min_val) < 0.0001:
        return np.asarray([1/len(series)for i in range(len(series))])
    else:
        return (series - np.mean(series))/(max_val - min_val)

def postive_normalization(series):
    max_val = max(series)
    min_val = min(series)
    if abs(max_val - min_val) < 0.0001:
        return np.asarray([1/len(series)for i in range(len(series))])
    else:
        nseries = series + 2 * abs(max(series))
        return nseries/np.sum(nseries)

def allocate_volume(volume, ratio_series, volume_series):
    while volume != 0:
        for (index, ), ratio in np.ndenumerate(ratio_series):
            expected_volume = max(1, int(volume * ratio))
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

def divide_according_price(price_series, volume_series, volume, price):
    delta_price_series = max_min_normalization(price - price_series)
    volume_normalization_series = (volume_series / max(volume_series)).astype(float)
    delta_volume_series = max_min_normalization(volume_normalization_series)
    ratio_series = postive_normalization(0.1 * delta_price_series + 0.9 * delta_volume_series)
    return allocate_volume(volume, ratio_series, volume_series)

def divide_according_position(position_series, volume_series, volume, position):
    delta_position_series = max_min_normalization((position - position_series).astype(np.float32))
    volume_normalization_series = (volume_series / max(volume_series)).astype(np.float32)
    delta_volume_series = max_min_normalization(volume_normalization_series)
    ratio_series = postive_normalization(0.1 * delta_position_series + 0.9 * delta_volume_series)
    return allocate_volume(volume, ratio_series, volume_series)

def number_of_days(pre_pos, pos):
    return pos - pre_pos

def divide_data(mdata, pos, price):
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

def divide_volume(volume, s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total, volume_total):
    max_volume = max(s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total)
    if s_p_volume_total == max_volume:
        l_p_volume = int(volume * (l_p_volume_total / volume_total))
        l_u_volume = int(volume * (l_u_volume_total / volume_total))
        s_u_volume = int(volume * (s_u_volume_total / volume_total))
        s_p_volume = volume - s_u_volume - l_p_volume - l_u_volume
    elif s_u_volume_total == max_volume:
        s_p_volume = int(volume * (s_p_volume_total / volume_total))
        l_p_volume = int(volume * (l_p_volume_total / volume_total))
        l_u_volume = int(volume * (l_u_volume_total / volume_total))
        s_u_volume = volume - s_p_volume - l_p_volume - l_u_volume
    elif l_p_volume_total == max_volume:
        s_p_volume = int(volume * (s_p_volume_total / volume_total))
        l_u_volume = int(volume * (l_u_volume_total / volume_total))
        s_u_volume = int(volume * (s_u_volume_total / volume_total))
        l_p_volume = volume - s_p_volume - s_u_volume - l_u_volume
    else:
        l_p_volume = int(volume * (l_p_volume_total / volume_total))
        s_p_volume = int(volume * (s_p_volume_total / volume_total))
        s_u_volume = int(volume * (s_u_volume_total / volume_total))
        l_u_volume = volume - s_p_volume - s_u_volume - l_p_volume
    return s_p_volume, s_u_volume, l_p_volume, l_u_volume

def adjust_volume(mdata, pos, volume, price, pre_outstanding, outstanding): 
    if pre_outstanding != outstanding:
        mdata['volume'] = evenly_distributed_chip(mdata['volume'], pre_outstanding, outstanding)

    s_p_data, s_u_data, l_p_data, l_u_data = divide_data(mdata, pos, price)

    #total volume
    volume_total = outstanding
    s_p_volume_total = np.sum(s_p_data['volume'])
    s_u_volume_total = np.sum(s_u_data['volume'])
    l_p_volume_total = np.sum(l_p_data['volume'])
    l_u_volume_total = np.sum(l_u_data['volume'])

    s_p_volume, s_u_volume, l_p_volume, l_u_volume = divide_volume(volume, s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total, volume_total)
    if s_p_volume > 0:
        s_p_data['volume'] = divide_according_price(s_p_data['price'], s_p_data['volume'], s_p_volume, price)
        if len(s_p_data[np.where(s_p_data['volume'] < 0)]) > 0:
            print("s_p_volume not equal")
            import pdb
            pdb.set_trace()
    if s_u_volume > 0:
        s_u_data['volume'] = divide_according_position(s_u_data['pos'], s_u_data['volume'], s_u_volume, pos)
        if len(s_u_data[np.where(s_u_data['volume'] < 0)]) > 0:
            print("s_u_volume not equal")
            import pdb
            pdb.set_trace()
    if l_p_volume > 0:
        l_p_data['volume'] = divide_according_price(l_p_data['price'], l_p_data['volume'], l_p_volume, price)
        if len(l_p_data[np.where(l_p_data['volume'] < 0)]) > 0:
            print("l_p_volume not equal")
            import pdb
            pdb.set_trace()
    if l_u_volume > 0:
        l_u_data['volume'] = divide_according_position(l_u_data['pos'], l_u_data['volume'], l_u_volume, pos)
        if len(l_u_data[np.where(l_u_data['volume'] < 0)]) > 0:
            print("l_u_volume not equal")
            import pdb
            pdb.set_trace()
    return np.concatenate((s_p_data, s_u_data, l_p_data, l_u_data), axis = 0)

def compute_oneday_distribution(pre_date_dist, cdate, pos, volume, aprice, pre_outstanding, outstanding):
    np_pre_data = pre_date_dist.to_records(index = False)
    np_pre_data = np_pre_data.astype(DTYPE_LIST)
    np_pre_data = adjust_volume(np_pre_data, pos, volume, aprice, pre_outstanding, outstanding)
    np_pre_data['date'] = cdate
    np_pre_data['outstanding'] = outstanding
    np_pre_data = np.concatenate((np_pre_data, np.array([(pos, cdate, cdate, aprice, volume, outstanding)], dtype = DTYPE_LIST)), axis=0)
    if np_pre_data['volume'].sum() != outstanding:
        print("one day after volume mismatched")
    df = DataFrame(data = np_pre_data, columns = CHIP_COLUMNS)
    df = df[df.volume != 0]
    df.date = df.date.str.decode('utf-8')
    df.sdate = df.sdate.str.decode('utf-8')
    df.price = df.price.astype(float).round(2)
    return df.reset_index(drop = True)

def compute_distribution(data):
    pre_outstanding = 0
    open_price = data.at[0, 'open']
    data = data[['date', 'volume', 'aprice', 'outstanding']]
    with pd.option_context('mode.chained_assignment', None):
        data['date'] = data['date'].str.encode("UTF-8")
    np_data = data.values
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
        df.date = df.date.str.decode('utf-8')
        df.sdate = df.sdate.str.decode('utf-8')
        df.price = df.price.astype(float).round(2)
    return df

def divide_volume_ori(volume, s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total, volume_total):
    if s_p_volume_total == max(s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total):
        l_p_volume = int(volume * max(0.30, l_p_volume_total / volume_total))
        l_u_volume = int(volume * max(0.08, l_u_volume_total / volume_total))
        s_u_volume = int(volume * max(0.02, s_u_volume_total / volume_total))
        s_p_volume = volume - s_u_volume - l_p_volume - l_u_volume
    elif s_u_volume_total == max(s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total):
        l_p_volume = int(volume * max(0.35, l_p_volume_total / volume_total))
        s_p_volume = int(volume * max(0.25, s_p_volume_total / volume_total))
        l_u_volume = int(volume * max(0.10, l_u_volume_total / volume_total))
        s_u_volume = volume - s_p_volume - l_p_volume - l_u_volume
    elif l_p_volume_total == max(s_p_volume_total, s_u_volume_total, l_p_volume_total, l_u_volume_total):
        s_p_volume = int(max(0.30 * volume, s_p_volume_total))
        l_u_volume = int(max(0.08 * volume, l_u_volume_total))
        s_u_volume = int(max(0.02 * volume, s_u_volume_total))
        l_p_volume = volume - s_p_volume - s_u_volume - l_u_volume
    else:
        l_p_volume = int(max(0.30 * volume, l_p_volume_total))
        s_p_volume = int(max(0.20 * volume, s_p_volume_total))
        s_u_volume = int(max(0.15 * volume, s_u_volume_total))
        l_u_volume = volume - s_p_volume - s_u_volume - l_p_volume
    return s_p_volume, s_u_volume, l_p_volume, l_u_volume

def average_distribute(volume_series, volume):
    start_total_volume = np.sum(volume_series)
    end_total_volume = start_total_volume - volume
    volume_series -= (volume_series * (volume/start_total_volume)).astype(int)
    real_total_volume = np.sum(volume_series)
    delta_sum = end_total_volume - real_total_volume
    while delta_sum > volume_series.size:
        volume_series -= int(delta_sum / volume_series.size)
        delta_sum = volume - np.sum(volume_series)
        if delta_sum == 0: return volume_series
    volume_series[np.argpartition(volume_series, delta_sum)[delta_sum:]] -= 1
    return volume_series
