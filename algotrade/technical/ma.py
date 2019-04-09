#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
def ma(df, n, key = 'close', name = 'ma'):
    """calculate the moving average for the given data.
    :param df: pandas.DataFrame
    :param n:
    :return: pandas.DataFrame
    """
    ma = pd.Series(df[key].rolling(n, min_periods=n).mean(), name = '%s_%s' % (name, str(n)))
    df = df.join(ma)
    return df

def sma(data, ndays): 
    # simple moving average 
    sma = pd.Series(data['close'].rolling(ndays).mean(), name = 'sma_%s' % ndays)
    data = data.join(sma) 
    return data

def ewma(data, ndays): 
    # exponentially-weighted moving average
    ewma = pd.Series(data['close'].ewm(span = ndays, min_periods = ndays).mean(), name = 'ewma_%s' % ndays) 
    data = data.join(ewma) 
    return data

def macd(data, nfast = 12, nslow = 26, mid = 9):
    data = ewma(data, nfast)
    data = ewma(data, nslow)
    dif = pd.Series(data['ewma_%s' % nfast] - data['ewma_%s' % nslow], name = 'dif')
    dea = pd.Series(dif.ewm(span = mid, min_periods = mid).mean(), name = 'dea')
    macd = pd.Series((dif - dea) * 2, name = 'macd')
    data = data.join(dif) 
    data = data.join(dea) 
    data = data.join(macd) 
    return data
