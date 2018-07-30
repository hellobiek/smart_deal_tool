#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import const as ct
from cstock import CStock 
class FeatureTool:
    def MACD(data, fastperiod=12, slowperiod=26, signalperiod=9):
        ewma12 = pd.ewma(price, span=fastperiod)
        ewma60 = pd.ewma(price, span=slowperiod)
        dif = ewma12-ewma60
        dea = pd.ewma(dif,span=signalperiod)
        bar = (dif-dea) #有些地方的bar = (dif-dea)*2，但是talib中MACD的计算是bar = (dif-dea)*1
        return dif,dea,bar

    def VMACD(data, fastperiod=12, slowperiod=26, signalperiod=9):
        svolume = sum(volume)
        vprice = np.array(price) *  np.array(volume)
        vprice = vprice / svolume
        return MACD(vprice, fastperiod, slowperiod, signalperiod)
    
    def MA(data, peried):
        return pd.rolling_mean(price, peried)
    
    def VMA(data, peried):
        svolume = sum(volume)
        vprice = np.array(price) *  np.array(volume)
        vprice = vprice / svolume
        return pd.rolling_mean(vprice, peried)

    #Bollinger Bands 
    def BBANDS(data, ndays):
        ma = pd.Series(pd.rolling_mean(data['close'], ndays)) 
        sd = pd.Series(pd.rolling_std(data['close'], ndays))
        b1 = ma + (2 * sd)
        B1 = pd.Series(b1, name = 'Upper BollingerBand') 
        data = data.join(B1) 
        b2 = ma - (2 * sd)
        B2 = pd.Series(b2, name = 'Lower BollingerBand') 
        data = data.join(B2) 
        return data

if __name__ == "__main__":
    cstock = CStock(ct.DB_INFO, "601318")
    data = cstock.get_k_data()
    ftool = FeatureTool()
    ftool.MACD(data)
