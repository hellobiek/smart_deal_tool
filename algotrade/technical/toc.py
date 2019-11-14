# -*- coding: utf-8 -*-
import pandas as pd
def toc(data, n = 12, average = 6):
    N = data['turnover'].diff(n)
    D = data['turnover'].shift(n)
    toc = pd.Series(N/D, name='toc')
    toc_ma = pd.Series(toc.rolling(average, min_periods=average).mean(), name = 'toc_ma')
    data = data.join(toc)
    data = data.join(toc_ma)
    return data 
