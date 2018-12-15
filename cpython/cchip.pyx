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
    delta = 1 if delta_sum > 0 else -1
    volume_series[np.argpartition(volume_series, abs(delta_sum))[:abs(delta_sum)]] += delta
    return volume_series

def divide_according_property(np.ndarray property_series, np.ndarray[long] volume_series, long total_volume, now_property):
    cdef long tmp_volume = 0
    cdef float holding_property = 0
    cdef float total_property = now_property * volume_series.size - np.sum(property_series)
    while total_volume != 0:
        tmp_volume = total_volume
        for (index, ), pro in np.ndenumerate(property_series):
            holding_property = now_property - pro
            expected_volume = max(1, long(tmp_volume * (holding_property / total_property)))
            if expected_volume > total_volume: expected_volume = total_volume
            total_volume -= min(volume_series[index], expected_volume)
            volume_series[index] = max(0, volume_series[index] - expected_volume)
            if 0 == total_volume: break
    return volume_series

def change_volume_for_long_unprofit(np.ndarray l_u_data, long volume, float price, long pos):
    return divide_according_property(l_u_data['pos'], l_u_data['volume'], volume, pos)

def change_volume_for_long_profit(np.ndarray l_p_data, long volume, float price, long pos):
    return divide_according_property(l_p_data['price'], l_p_data['volume'], volume, price)

def change_volume_for_short_unprofit(np.ndarray s_u_data, long volume, float price, long pos):
    return divide_according_property(s_u_data['pos'], s_u_data['volume'], volume, pos)

def change_volume_for_short_profit(np.ndarray s_p_data, long volume, float price, long pos):
    return divide_according_property(s_p_data['price'], s_p_data['volume'], volume, price)

def number_of_days(np.ndarray[long] pre_pos, long pos):
    return pos - pre_pos

def divide_data(np.ndarray mdata, long pos, float price):
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

def adjust_volume(np.ndarray mdata, long pos, long volume, float price, long pre_outstanding, long outstanding):
    if pre_outstanding != outstanding:
        mdata['volume'] = evenly_distributed_new_chip(mdata['volume'], pre_outstanding, outstanding)

    s_p_data, s_u_data, l_p_data, l_u_data = divide_data(mdata, pos, price)

    #total volume
    cdef long volume_total = outstanding
    cdef long s_p_volume_total = np.sum(s_p_data['volume'])
    cdef long s_u_volume_total = np.sum(s_u_data['volume'])
    cdef long l_p_volume_total = np.sum(l_p_data['volume'])
    cdef long l_u_volume_total = np.sum(l_u_data['volume'])

    cdef long s_p_volume = 0 #long(volume * (s_p_volume_total / volume_total))
    cdef long s_u_volume = 0 #long(volume * (s_u_volume_total / volume_total))
    cdef long l_p_volume = 0 #long(volume * (l_p_volume_total / volume_total)) 
    cdef long l_u_volume = 0 #volume - s_p_volume - s_u_volume - l_p_volume

    if s_p_data.size > 0 and s_u_data.size == 0 and l_p_data.size == 0 and l_u_data.size == 0:
        mdata['volume'] = change_volume_for_short_profit(mdata, volume, price, pos)
    elif s_p_data.size == 0 and s_u_data.size > 0 and l_p_data.size == 0 and l_u_data.size == 0:
        mdata['volume'] = change_volume_for_short_unprofit(mdata, volume, price, pos)
    elif s_p_data.size == 0 and s_u_data.size == 0 and l_p_data.size > 0 and l_u_data.size == 0:
        mdata['volume'] = change_volume_for_long_profit(mdata, volume, price, pos)
    elif s_p_data.size == 0 and s_u_data.size == 0 and l_p_data.size == 0 and l_u_data.size > 0:
        mdata['volume'] = change_volume_for_long_unprofit(mdata, volume, price, pos)
    elif s_p_data.size > 0 and s_u_data.size > 0 and l_p_data.size == 0 and l_u_data.size == 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        s_u_volume = volume - s_p_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        mdata = np.concatenate((s_p_data, s_u_data), axis = 0)
    elif s_p_data.size > 0 and s_u_data.size == 0 and l_p_data.size > 0 and l_u_data.size == 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        l_p_volume = volume - s_p_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        mdata = np.concatenate((s_p_data, l_p_data), axis = 0)
    elif s_p_data.size > 0 and s_u_data.size == 0 and l_p_data.size == 0 and l_u_data.size > 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        l_u_volume = volume - s_p_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_p_data, l_u_data), axis = 0)
    elif s_p_data.size == 0 and s_u_data.size > 0 and l_p_data.size == 0 and l_u_data.size > 0:
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_u_volume = volume - s_u_volume
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_u_data, l_u_data), axis = 0)
    elif s_p_data.size == 0 and s_u_data.size > 0 and l_p_data.size > 0 and l_u_data.size == 0:
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_p_volume = volume - s_u_volume
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        mdata = np.concatenate((s_u_data, l_p_data), axis = 0)
    elif s_p_data.size == 0 and s_u_data.size > 0 and l_p_data.size > 0 and l_u_data.size > 0:
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        l_u_volume = volume - s_u_volume - l_p_volume
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_u_data, l_p_data, l_u_data), axis = 0)
    elif s_p_data.size > 0 and s_u_data.size == 0 and l_p_data.size > 0 and l_u_data.size > 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        l_u_volume = volume - s_p_volume - l_p_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_p_data, l_p_data, l_u_data), axis = 0)
    elif s_p_data.size > 0 and s_u_data.size > 0 and l_p_data.size == 0 and l_u_data.size > 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_u_volume = volume - s_p_volume - s_u_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_p_data, s_u_data, l_u_data), axis = 0)
    elif s_p_data.size > 0 and s_u_data.size > 0 and l_p_data.size > 0 and l_u_data.size == 0:
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_p_volume = volume - s_p_volume - s_u_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        mdata = np.concatenate((s_p_data, s_u_data, l_p_data), axis = 0)
    else:#s_p_data.size > 0 and s_u_data.size > 0 and l_p_data.size > 0 and l_u_data.size > 0
        s_p_volume = long(volume * (s_p_volume_total / volume_total))
        s_u_volume = long(volume * (s_u_volume_total / volume_total))
        l_p_volume = long(volume * (l_p_volume_total / volume_total))
        l_u_volume = volume - s_p_volume - s_u_volume - l_p_volume
        s_p_data['volume'] = change_volume_for_short_profit(s_p_data, s_p_volume, price, pos)
        s_u_data['volume'] = change_volume_for_short_unprofit(s_u_data, s_u_volume, price, pos)
        l_p_data['volume'] = change_volume_for_long_profit(l_p_data, l_p_volume, price, pos)
        l_u_data['volume'] = change_volume_for_long_unprofit(l_u_data, l_u_volume, price, pos)
        mdata = np.concatenate((s_p_data, s_u_data, l_p_data, l_u_data), axis = 0)
    return mdata

def compute_oneday_distribution(pre_date_dist, cdate, pos, volume, aprice, pre_outstanding, outstanding):
    np_pre_data = pre_date_dist.to_records(index = False)
    np_pre_data = np_pre_data.astype(DTYPE_LIST)
    np_pre_data = adjust_volume(np_pre_data, pos, volume, aprice, pre_outstanding, outstanding)
    np_pre_data['date'] = cdate
    np_pre_data['outstanding'] = outstanding
    tdata = (pos, cdate, cdate, aprice, volume, outstanding)
    t = np.array([tdata], dtype = DTYPE_LIST)
    np_pre_data = np.concatenate((np_pre_data, np.array(t)), axis=0)
    df = DataFrame(data = np_pre_data, columns = CHIP_COLUMNS)
    df = df[df.volume != 0]
    df.date = df.date.str.decode('utf-8')
    df.sdate = df.sdate.str.decode('utf-8')
    df.price = df.price.astype(float)
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
            tmp_arrary = adjust_volume(tmp_arrary, _index, volume, aprice, pre_outstanding, outstanding)
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
