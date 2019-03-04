#coding=utf-8
import pandas as pd
def roc(data, n):
    N = data['close'].diff(n)
    D = data['close'].shift(n)
    roc = pd.Series(N/D,name='roc')
    data = data.join(roc)
    return data 
