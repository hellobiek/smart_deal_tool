#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
def MACD(price, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = pd.ewma(price, span=fastperiod)
    ewma60 = pd.ewma(price, span=slowperiod)
    dif = ewma12-ewma60
    dea = pd.ewma(dif,span=signalperiod)
    bar = (dif-dea) #有些地方的bar = (dif-dea)*2，但是talib中MACD的计算是bar = (dif-dea)*1
    return dif,dea,bar

def VMACD(price, volume, fastperiod=12, slowperiod=26, signalperiod=9):
    svolume = sum(volume)
    vprice = np.array(price) *  np.array(volume)
    vprice = vprice / svolume
    return MACD(vprice, fastperiod, slowperiod, signalperiod)

def MA(price, peried):
    return pd.rolling_mean(price, peried)

def VMA(price, volume, peried):
    svolume = sum(volume)
    vprice = np.array(price) *  np.array(volume)
    vprice = vprice / svolume
    return pd.rolling_mean(vprice, peried)
