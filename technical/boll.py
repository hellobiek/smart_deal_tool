#coding=utf-8
import pandas as pd
def boll(data, ndays):
    MA = pd.Series(data['close'].rolling(ndays).mean()) 
    SD = pd.Series(data['close'].rolling(ndays).std()) 
    b1 = MA + (2 * SD)
    B1 = pd.Series(b1, name = 'upper bollingerBand') 
    data = data.join(B1) 
    b2 = MA - (2 * SD)
    B2 = pd.Series(b2, name = 'lower bollingerBand') 
    data = data.join(B2) 
    return data
