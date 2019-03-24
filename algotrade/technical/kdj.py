#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
def sma(d, N):
    last = np.nan
    v = pd.Series(index=d.index)
    for key in d.index:
        x = d[key]
        x1 = (x + (N - 1) * last) / N if last == last else x
        last = x1
        v[key] = x1
        if x1 != x1: last = x
    return v

def kdj(data, N1=9, N2=3, N3=3):
    low  = data.low.rolling(N1).min()
    high = data.high.rolling(N1).max()
    rsv  = (data.close - low) / (high - low) * 100
    k = sma(rsv, N2)
    d = sma(k, N3)
    j = k * 3 - d * 2
    data['k'] = k
    data['d'] = d
    data['j'] = j
    return data
