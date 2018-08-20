#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import const as ct
from qfq import qfq
from common import get_market_name
from cstock import CStock

def MACD(data, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = data.ewm(fastperiod).mean()
    ewma26 = data.ewm(slowperiod).mean()
    dif = ewma12 - ewma26
    dea = dif.ewm(signalperiod).mean()
    bar = (dif - dea)   #有些地方的bar = (dif-dea)*2，但是talib中MACD的计算是bar = (dif-dea) * 1
    return dif, dea, bar

def VMACD(price, volume, fastperiod=12, slowperiod=26, signalperiod=9):
    svolume = sum(volume)
    vprice = np.array(price) *  np.array(volume)
    vprice = vprice / svolume
    return MACD(pd.Series(vprice), fastperiod, slowperiod, signalperiod)

def MA(data, peried):
    return data.rolling(peried).mean()

def VMA(amount, volume, peried = 5):
    svolume = sum(volume)
    samount = sum(amount)
    #return MA(pd.Series(vprice), peried)

if __name__ == "__main__":
    code = '601318'
    prestr = "1" if get_market_name(code) == "sh" else "0"
    cstock = CStock(ct.DB_INFO, code)
    data = cstock.get_k_data()
    data['close'] = data.amount/data.volume

    filename = "%s%s.csv" % (prestr, code)
    data = pd.read_csv("/data/tdx/history/days/%s" % filename, sep = ',')
    data = data[['date', 'open', 'low', 'high', 'close', 'volume', 'amount']]
    data = data.sort_index(ascending = False)
    data = data.reset_index(drop = True)

    info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    info = info[(info.code == code) & (info.type == 1)]
    info = info.sort_index(ascending = False)
    info = info.reset_index(drop = True)
    info = info[['money', 'price', 'count', 'rate', 'date']]

    data = qfq(data, code, info)
    data = data.sort_index(ascending = False)
    data = data.reset_index(drop = True)
    data['ma8'] = MA(data['close'], 8)
    data['ma24'] = MA(data['close'], 24)
    data['ma60'] = MA(data['close'], 60)
    data[["date", "close", "ma8", "ma24", "ma60"]].plot(figsiz=(10,18))
