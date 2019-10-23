# -*- coding: utf-8 -*-
import pandas as pd
def boll(data, ndays = 20):
    MA = pd.Series(data['close'].rolling(ndays).mean())
    SD = pd.Series(data['close'].rolling(ndays).std())
    b1 = MA + (2 * SD)
    B1 = pd.Series(b1, name = 'ub')
    data = data.join(B1)
    b2 = MA - (2 * SD)
    B2 = pd.Series(b2, name = 'lb')
    data = data.join(B2)
    B3 = pd.Series((b1 + b2)/2, name = 'mb')
    data = data.join(B3)
    return data
