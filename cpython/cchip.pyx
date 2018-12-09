# cython: language_level=3, boundscheck=False, wraparound=False, nonecheck=False, infer_types=True
import numpy as np
cimport numpy as np
from pandas import DataFrame

CHIP_COLUMNS = ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
DTYPE_LIST = [('pos', 'i8'), ('sdate', 'S10'), ('date', 'S10'), ('price', 'f4'), ('volume', 'i8'), ('outstanding', 'i8')]

def evenly_distributed_new_chip(np.ndarray[long] volume_series, long pre_outstanding, long outstanding):
    volume_series = (outstanding * (volume_series / pre_outstanding)).astype(long)
    cdef long delta = 0
    cdef long real_total_volume = np.sum(volume_series)
    cdef long delta_sum = outstanding - real_total_volume
    while abs(delta_sum) > volume_series.size:
        volume_series += long(delta_sum / volume_series.size)
        delta_sum = outstanding - np.sum(volume_series)
        if delta_sum == 0: return volume_series
    delta = -1 if delta_sum > 0 else 1
    volume_series[np.argpartition(volume_series, abs(delta_sum))[abs(delta_sum):]] += delta
    return volume_series

def average_distribute(np.ndarray[long] volume_series, long volume):
    cdef long start_total_volume = np.sum(volume_series)
    cdef long end_total_volume = start_total_volume - volume
    volume_series -= (volume_series * (volume/start_total_volume)).astype(long)
    cdef long real_total_volume = np.sum(volume_series)
    cdef long delta_sum = end_total_volume - real_total_volume
    while delta_sum > volume_series.size:
        volume_series -= long(delta_sum / volume_series.size)
        delta_sum = volume - np.sum(volume_series)
        if delta_sum == 0: return volume_series
    volume_series[np.argpartition(volume_series, delta_sum)[delta_sum:]] -= 1
    return volume_series

def divide_according_property(np.ndarray property_series, np.ndarray[long] volume_series, long total_volume, now_property):
    property_series = np.sort(property_series)
    cdef float holding_property = 0
    cdef float total_property = now_property * volume_series.size - np.sum(property_series)
    while total_volume != 0:
        for (_index, ), pro in np.ndenumerate(property_series):
            holding_property = now_property - pro
            expected_volume = max(1, long(total_volume * (holding_property / total_property)))
            if expected_volume > total_volume: expected_volume = total_volume
            total_volume -= min(volume_series[_index], expected_volume)
            volume_series[_index] = max(0, volume_series[_index] - expected_volume)
            if 0 == total_volume: break
    return volume_series

def change_volume_for_short(np.ndarray mdata, long volume, float price, long pos):
    profit_data = mdata[mdata['price'] < price]
    unprofit_data = mdata[mdata['price'] >= price]
    if profit_data.size == 0:
        return average_distribute(unprofit_data['volume'], volume)

    if unprofit_data.size == 0:
        return divide_according_property(profit_data['price'], profit_data['volume'], volume, price)

    cdef long total_volume = np.sum(mdata['volume'])
    cdef long u_total_volume = np.sum(unprofit_data['volume'])

    cdef long u_volume = long(volume * (u_total_volume/total_volume))
    cdef long p_volume = volume - u_volume

    profit_data['volume'] = divide_according_property(profit_data['price'], profit_data['volume'], p_volume, price)
    unprofit_data['volume'] = average_distribute(unprofit_data['volume'], u_volume)

    profit_data = np.concatenate((profit_data, unprofit_data), axis = 0)
    return profit_data['volume']

def change_volume_for_long(np.ndarray mdata, long volume, float price, long pos):
    profit_data = mdata[mdata['price'] < price]
    unprofit_data = mdata[mdata['price'] >= price]
    if profit_data.size == 0:
        return divide_according_property(unprofit_data['pos'], unprofit_data['volume'], volume, pos)

    if unprofit_data.size == 0:
        return average_distribute(profit_data['volume'], volume)

    cdef long total_volume = np.sum(mdata['volume'])
    cdef long u_total_volume = np.sum(unprofit_data['volume'])
    cdef long u_volume = long(volume * (u_total_volume/total_volume))
    cdef long p_volume = volume - u_volume

    profit_data['volume'] = average_distribute(profit_data['volume'], p_volume)
    unprofit_data['volume'] = divide_according_property(unprofit_data['pos'], unprofit_data['volume'], u_volume, pos)
    profit_data = np.concatenate((profit_data, unprofit_data), axis = 0)
    return profit_data['volume']

def number_of_days(np.ndarray[long] pre_pos, long pos):
    return pos - pre_pos

def adjust_volume(np.ndarray mdata, long pos, long volume, float price, long pre_outstanding, long outstanding):
    if pre_outstanding != outstanding:
        mdata['volume'] = evenly_distributed_new_chip(mdata['volume'], pre_outstanding, outstanding)

    #short chip data
    s_data = mdata[np.apply_along_axis(number_of_days, 0, mdata['pos'], pos) <= 60]

    #very long chip data
    l_data = mdata[np.apply_along_axis(number_of_days, 0, mdata['pos'], pos) > 60]

    if l_data.size == 0:
        return change_volume_for_short(s_data, volume, price, pos)

    #short term volume
    cdef long s_volume_total = np.sum(s_data['volume'])
   
    #long term volume
    cdef long l_volume_total = np.sum(l_data['volume'])

    #total volume
    cdef long volume_total = s_volume_total + l_volume_total
    cdef long s_volume = long(volume * (s_volume_total / volume_total))
    cdef long l_volume = volume - s_volume

    #change short volume rate
    s_data['volume'] = change_volume_for_short(s_data, s_volume, price, pos)

    #change long volume rate
    l_data['volume'] = change_volume_for_long(l_data, l_volume, price, pos)

    s_data = np.concatenate((s_data, l_data), axis = 0)
    return s_data['volume']

def compute_oneday_distribution(pre_date_dist, cdate, pos, volume, aprice, pre_outstanding, outstanding):
    np_pre_data = pre_date_dist.to_records(index = False)
    np_pre_data = np_pre_data.astype(DTYPE_LIST)
    np_pre_data['volume'] = adjust_volume(np_pre_data, pos, volume, aprice, pre_outstanding, outstanding)
    np_pre_data['date'] = cdate
    np_pre_data['outstanding'] = outstanding
    tdata = (pos, cdate, cdate, aprice, volume, outstanding)
    t = np.array([tdata], dtype = DTYPE_LIST)
    np_pre_data = np.concatenate((np_pre_data, np.array(t)), axis=0)
    df = DataFrame(data = np_pre_data, columns = CHIP_COLUMNS)
    df = df[df.volume != 0]
    return df.reset_index(drop = True)

def compute_distribution(data):
    cdef char *cdate
    cdef float aprice, open_price = data.at[0, 'open']
    cdef long pos, volume, _index, outstanding, pre_outstanding = 0
    data = data[['date', 'volume', 'aprice', 'outstanding']]
    data.date = data.date.str.encode("UTF-8")
    np_data = data.values
    tmp_arrary = np.zeros((2, 6), dtype = DTYPE_LIST)
    data_arrary = np.zeros((2, 6), dtype = DTYPE_LIST)
    for _index, row in enumerate(np_data):
        cdate, volume, aprice, outstanding = row[[0, 1, 2, 3]]
        if 0 == _index:
            t1 = (_index, cdate, cdate, aprice, volume, outstanding)
            t2 = (_index, cdate, cdate, open_price, outstanding - volume, outstanding)
            t = np.array([t1, t2], dtype = DTYPE_LIST)
            tmp_arrary = t.copy()
        else:
            tmp_arrary['volume'] = adjust_volume(tmp_arrary, _index, volume, aprice, pre_outstanding, outstanding)
            tmp_arrary['date'] = cdate
            tmp_arrary['outstanding'] = outstanding
            tdata = (_index, cdate, cdate, aprice, volume, outstanding)
            t = np.array([tdata], dtype = DTYPE_LIST)
            tmp_arrary = np.concatenate((tmp_arrary, np.array(t)), axis=0)
        pre_outstanding = outstanding
        tmp_arrary = tmp_arrary[tmp_arrary['volume'] > 0]
        data_arrary = tmp_arrary.copy() if 0 == _index else np.concatenate((data_arrary, tmp_arrary), axis = 0)
    df = DataFrame(data = data_arrary, columns = CHIP_COLUMNS)
    df.date = df.date.str.decode('utf-8')
    df.sdate = df.sdate.str.decode('utf-8')
    df.price = df.price.astype(float)
    return df
