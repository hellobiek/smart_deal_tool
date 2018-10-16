#-*- coding: utf-8 -*-
import const as ct
import numpy as np
import pandas as pd
#def movingaverage(x, N):
#    return x.rolling(N).mean()
#
#def ExpMovingAverage(values, window):
#    weights = np.exp(np.linspace(-1., 0., window))
#    weights /= weights.sum()
#    a =  np.convolve(values, weights, mode='full')[:len(values)]
#    a[:window] = a[window]
#    return a
#
#nema  = 9
#nfast = 12
#nslow = 26
#emaslow, emafast, macd = computeMACD(k_data.close.values)
#ema9 = ExpMovingAverage(macd, nema)
#def computeMACD(x, slow=26, fast=12):
#    """
#    compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg'
#    return value is emaslow, emafast, macd which are len(x) arrays
#    """
#    emaslow = ExpMovingAverage(x, slow)
#    emafast = ExpMovingAverage(x, fast)
#    return emaslow, emafast, emafast - emaslow

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
    break_index_lists = df.loc[df.breakup != 0].index.tolist()
    #compute price base and price change
    s_index = 0
    df['base'] = 0
    df['pday'] = 0
    for e_index in break_index_lists:
        direction = df.loc[e_index, 'breakup']
        ppchange = 0.9 if direction > 0 else 1.1
        base = df.loc[s_index:e_index - 1, 'uprice'].max() if direction > 0 else df.loc[s_index:e_index - 1, 'uprice'].min()
        df.at[s_index:e_index-1, 'base'] = base
        df.at[s_index:e_index-1, 'ppchange'] = ppchange 
        df.at[s_index:e_index-1, 'pday'] = -1 * direction * (df.loc[s_index:e_index-1].index - s_index + 1)
        s_index = e_index
        if e_index == break_index_lists[-1]:
            direction = df.loc[e_index, 'breakup']
            ppchange = 1.1 if direction > 0 else 0.9
            base = df.loc[e_index:, 'uprice'].max() if direction < 0 else df.loc[e_index:, 'uprice'].min()
            df.at[e_index:, 'base'] = base
            df.at[e_index:, 'ppchange'] = ppchange
            df.at[e_index:, 'pday'] = direction * (df.loc[e_index:].index - e_index + 1)
    #compute the base floating profit
    df['profit'] = (np.log(df.uprice) - np.log(df.base)).abs() / np.log(df.ppchange)
    #drop the unnessary columns
    df = df.drop(['base','ppchange','breakup'], axis=1)
    return df

def ProChip_NeiChip(df, dist_data, mdate = None):
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
            n_val = 100 * group[(group.price < close_price * 1.08) & (group.price > close_price * 0.92)].volume.sum() / outstanding
            p_profit_vol_list.append(p_val)
            p_neighbor_vol_list.append(n_val)
        df['ppercent'] = p_profit_vol_list
        df['npercent'] = p_neighbor_vol_list
    else:
        p_close     = df['close'].values[0]
        outstanding = df['outstanding'].values[0]
        p_val = 100 * dist_data[dist_data.price < p_close].volume.sum() / outstanding
        n_val = 100 * dist_data[(dist_data.price < p_close * 1.08) & (dist_data.price > p_close * 0.92)].volume.sum() / outstanding
        df['ppercent'] = p_val
        df['npercent'] = n_val
    return df
        
#function           : u-limitted t-day moving avering price
#input data columns : ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
def Mac(df, data, peried = 0):
    ulist = list()
    for name, group in data.groupby(data.date):
        if peried != 0 and len(group) > peried:
            group = group.nlargest(peried, 'pos')
        total_volume = group.volume.sum()
        total_amount = group.price.dot(group.volume)
        ulist.append(total_amount / total_volume)
    df['uprice'] = ulist
    return df

def RelativeIndexStrength(df, index_df, cdate = None, preday_sri = None):
    if cdate is None:
        df['sai'] = 0 
        s_pchange = (df['close'] - df['preclose']) / df['preclose']
        i_pchange = (index_df['close'] - index_df['preclose']) / index_df['preclose']
        df['sri'] = 100 * (s_pchange - i_pchange)
        df.at[df.sri > 0, 'sai'] = df.loc[df.sri > 0, 'sri']
        df['sri'] = df['sri'].cumsum()
    else:
        s_pchange = (df.loc[df.date == cdate, 'close'] - df.loc[df.date == cdate, 'preclose']) / df.loc[df.date == cdate, 'preclose']
        s_pchange = s_pchange.values[0]
        i_pchange = (index_df.loc[index_df.date == cdate, 'close'] - index_df.loc[index_df.date == cdate, 'preclose']) / index_df.loc[index_df.date == cdate, 'preclose']
        i_pchange = i_pchange.values[0]
        df['sai'] = 100 * (s_pchange - i_pchange) if s_pchange > 0 and i_pchange < 0 else 0
        df['sri'] = preday_sri + 100 * (s_pchange - i_pchange)
    return df 
