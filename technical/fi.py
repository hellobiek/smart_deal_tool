#coding=utf-8
import pandas as pd
def fi(data, ndays):
    #pd.Series(df['close'].diff(n) * df['volume'].diff(n), name = 'Force_' + str(n))
    fi = pd.Series(data['close'].diff(ndays).pct_change() * data['volume'].diff(ndays).pct_change() * 100, name = 'fi_%s' % ndays)
    data = data.join(fi) 
    return data
