#coding=utf-8
import pandas as pd
def cci(data, ndays):
    tp = (data['high'] + data['low'] + data['close']) / 3
    cci = pd.Series((tp - tp.rolling(ndays).mean()) / (0.015 * tp.rolling(ndays).std()), name = 'cci')
    data = data.join(cci)
    return data
