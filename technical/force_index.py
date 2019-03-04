#coding=utf-8
import pandas as pd
def force_index(data, ndays):
    fi = pd.Series(data['close'].diff(ndays) * data['volume'], name = 'force_index') 
    data = data.join(fi) 
    return data
