#-*- coding: utf-8 -*-
import pandas as pd
def atr(data, ndays = 8):
    def func(low, high, perclose):
        return max(high - low, abs(low - perclose), abs(high - perclose))
    atr = pd.Series(data.apply(lambda data: func(data['low'], data['high'], data['preclose']), axis = 1).rolling(ndays).mean(), name = 'atr')
    data = data.join(atr)
    return data
