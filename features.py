#-*- coding: utf-8 -*-
import const as ct
import numpy as np
import pandas as pd

def MACD(data, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = data.ewm(fastperiod).mean()
    ewma26 = data.ewm(slowperiod).mean()
    dif = ewma12 - ewma26
    dea = dif.ewm(signalperiod).mean()
    bar = (dif - dea)   #有些地方的bar = (dif-dea)*2，但是talib中MACD的计算是bar = (dif-dea) * 1
    return dif, dea, bar

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

def BaseFloatingProfit(df, mdate = None, num = 60):
    #get all breakup points
    df['breakup'] = 0
    df.at[(df.preclose < df.uprice) & (df.close > df.uprice), 'breakup'] = 1
    df.at[(df.preclose > df.uprice) & (df.close < df.uprice), 'breakup'] = -1
    break_index_lists = df.loc[df.breakup != 0].index.tolist()
    #get all fake break points
    should_remove_index_list = list()
    for break_index in range(len(break_index_lists)):
        if break_index < len(break_index_lists) - 1:
            if break_index_lists[break_index + 1] - break_index_lists[break_index] < num:
                should_remove_index_list.append(break_index_lists[break_index])
        else:
            if len(df) - break_index_lists[break_index] < num:
                should_remove_index_list.append(break_index_lists[break_index])
    df.at[df.index.isin(should_remove_index_list), 'breakup'] = 0
    #merge break points to better points
    s_index = 0
    should_remove_index_list = list()
    break_index_lists = df.loc[df.breakup != 0].index.tolist()
    for break_index in range(1, len(break_index_lists)):
        if df.loc[break_index_lists[s_index], 'breakup'] != df.loc[break_index_lists[break_index], 'breakup']:
            s_index = break_index
        else:
            should_remove_index_list.append(break_index_lists[break_index])
    df.at[df.index.isin(should_remove_index_list), 'breakup'] = 0
    break_index_list = df.loc[df.breakup != 0].index.tolist()
    #compute price base and price change
    s_index = 0
    df['base'] = 0
    for e_index in break_index_lists:
        direction = df.loc[e_index, 'breakup']
        pchange = 0.9 if direction > 0 else 1.1
        base = df.loc[s_index:e_index - 1, 'uprice'].max() if direction > 0 else df.loc[s_index:e_index - 1, 'uprice'].min()
        df.at[s_index:e_index-1, 'base'] = base
        df.at[s_index:e_index-1, 'pchange'] = pchange 
        s_index = e_index
        if e_index == break_index_lists[-1]:
            direction = df.loc[e_index, 'breakup']
            pchange = 1.1 if direction > 0 else 0.9
            base = df.loc[e_index:, 'uprice'].max() if direction < 0 else df.loc[e_index:, 'uprice'].min()
            df.at[e_index:, 'base'] = base
            df.at[e_index:, 'pchange'] = pchange
    #compute the base floating profit
    df['profit'] = (np.log(df.uprice) - np.log(df.base)).abs() / np.log(df.pchange)
    #drop the unnessary columns
    df = df.drop(['base','pchange', 'breakup'], axis=1)
    return df

def GameKline(df, dist_data, mdate = None):
    if mdate is None:
        p_close_vol_list = list()
        groups = dist_data.groupby(dist_data.date)
        for _index, cdate in df.cdate.iteritems():
            drow = df.loc[_index]
            p_close = drow['close']
            outstanding = drow['outstanding']
            group = groups.get_group(cdate)
            val = 100 * group[group.price < p_close].volume.sum() / outstanding
            p_close_vol_list.append(val)
        df['gline'] = p_close_vol_list
    else:
        groups = dist_data.groupby(dist_data.date)
        group = groups.get_group(mdate)
        drow = df.loc[df.date == mdate]
        p_close = drow['close']
        outstanding = drow['outstanding']
        val = 100 * group[group.price < p_close].volume.sum() / outstanding
        df.at[df.date == mdate, 'gline'] = val
    return df
        
#function           : u-limitted t-day moving avering price
#input data columns : ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
def Mac(df, peried = 0):
    ulist = list()
    df = df.sort_values(by = 'date', ascending= True)
    for name, group in df.groupby(df.date):
        if peried != 0 and len(group) > peried:
            group = group.nlargest(peried, 'pos')
        total_volume = group.volume.sum()
        total_amount = group.price.dot(group.volume)
        ulist.append(total_amount / total_volume)
    return ulist
