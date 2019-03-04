#-*- coding: utf-8 -*-
import pandas as pd
def sma(data, ndays): 
    # simple moving average 
    sma = pd.Series(data['close'].rolling(ndays).mean(), name = 'sma_%s' % ndays)
    data = data.join(sma) 
    return data

def ewma(data, ndays): 
    # exponentially-weighted moving average
    ewma = pd.Series(data['close'].ewm(com = ndays).mean(), name = 'ewma_%s' % ndays) 
    data = data.join(ewma) 
    return data

def macd(data, nslow, nfast):
    data = ewma(data, nslow)
    data = ewma(data, nfast)
    macd = pd.Series(data['ewma_%s' % nfast] - data['ewma_%s' % nslow], name = 'macd')
    data = data.join(macd) 
    return data
