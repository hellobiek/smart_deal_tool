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

def get_effective_breakup_index(break_index_lists, num, df):
    break_index = 0
    effective_breakup_index_list = list()
    while break_index < len(break_index_lists):
        pre_price = df.at[break_index_lists[break_index], 'uprice']
        if break_index < len(break_index_lists) - 1:
            now_price = df.at[break_index_lists[break_index + 1],'uprice']
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if break_index_lists[break_index + 1] - break_index_lists[break_index] > num:
                    if df.at[break_index_lists[break_index + 1], 'breakup'] * df.at[break_index_lists[break_index], 'breakup'] > 0: raise("get error ip failed")
                    effective_breakup_index_list.append(break_index_lists[break_index])
        else:
            now_price = df.at[len(df) - 1, 'uprice']
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if len(df) - break_index_lists[break_index] > num:
                    effective_breakup_index_list.append(break_index_lists[break_index])
        break_index += 1
    return effective_breakup_index_list

def get_breakup_data(df):
    df['pos'] = 0
    df.at[df.close > df.uprice, 'pos'] = 1
    df.at[df.close < df.uprice, 'pos'] = -1
    df['pre_pos'] = df['pos'].shift(1)
    df.at[0, 'pre_pos'] = 0
    df['pre_pos'] = df['pre_pos'].astype(int)
    df['breakup'] = 0
    df.at[(df.pre_pos <= 0) & (df.pos > 0), 'breakup'] = 1
    df.at[(df.pre_pos >= 0) & (df.pos < 0), 'breakup'] = -1
    df = df.drop(['pos', 'pre_pos'],  axis=1)
    return df

def base_floating_profit(df, num, mdate = None):
    if mdate is None:
        df = get_breakup_data(df)
        break_index_lists = df.loc[df.breakup != 0].index.tolist()
        effective_breakup_index_list = get_effective_breakup_index(break_index_lists, num, df)
        df['pday'] = 1
        df['base'] = df['close']
        df['ppchange'] = 0
        if len(effective_breakup_index_list) == 0:
            df['profit'] = (df.close - df.uprice) / df.uprice
        else:
            s_index = 0
            for e_index in effective_breakup_index_list:
                if s_index == e_index:
                    if len(effective_breakup_index_list) == 1:
                        base = df.loc[s_index, 'uprice']
                        direction = df.loc[s_index, 'breakup']
                        ppchange = 1.1 if direction > 0 else 0.9
                        df.at[s_index:, 'base'] = base
                        df.at[s_index:, 'ppchange'] = ppchange
                        df.at[s_index:, 'pday'] = direction * (df.loc[s_index:].index - s_index + 1)
                else:
                    base = df.loc[s_index, 'uprice']
                    direction = df.loc[e_index, 'breakup']
                    ppchange = 1.1 if direction < 0 else 0.9
                    df.at[s_index:e_index - 1, 'base'] = base
                    df.at[s_index:e_index - 1, 'ppchange'] = ppchange
                    df.at[s_index:e_index - 1, 'pday'] = -1 * direction * (df.loc[s_index:e_index - 1].index - s_index + 1)
                    s_index = e_index
                    if e_index == effective_breakup_index_list[-1]:
                        base = df.loc[e_index, 'uprice']
                        direction = df.loc[e_index, 'breakup']
                        ppchange = 1.1 if direction > 0 else 0.9
                        df.at[e_index:, 'base'] = base
                        df.at[e_index:, 'ppchange'] = ppchange
                        df.at[e_index:, 'pday'] = direction * (df.loc[e_index:].index - e_index + 1)
            df['profit'] = (np.log(df.close) - np.log(df.base)).abs() / np.log(df.ppchange)
    return df[['date', 'profit', 'pday']]
