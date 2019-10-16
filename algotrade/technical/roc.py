# -*- coding: utf-8 -*-
import pandas as pd
def roc(data, n = 12, average = 6):
    N = data['close'].diff(n)
    D = data['close'].shift(n)
    roc = pd.Series(N/D, name='roc')
    roc_ma = pd.Series(roc.rolling(average, min_periods=average).mean(), name = 'roc_ma')
    data = data.join(roc)
    data = data.join(roc_ma)
    return data 
