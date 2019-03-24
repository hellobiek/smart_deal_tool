#coding=utf-8
import pandas as pd
def emv(data, ndays): 
    dm = ((data['high'] + data['low'])/2) - ((data['high'].shift(1) + data['low'].shift(1))/2)
    br = (data['volume'] / 100000000) / ((data['high'] - data['low']))
    emv = dm / br 
    emv_ma = pd.Series(emv.rolling(ndays).mean(), name = 'emv') 
    data = data.join(emv_ma) 
    return data 
