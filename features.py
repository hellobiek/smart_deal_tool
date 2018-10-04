#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import const as ct
from qfq import qfq
from common import get_market_name, number_of_days
#from cstock import CStock

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

def SMA(d, N):
    last = np.nan
    v = pd.Series(index=d.index)
    for key in d.index:
        x = d[key]
        x1 = (x + (N - 1) * last) / N if last == last else x
        last = x1
        v[key] = x1
        if x1 != x1: last = x
    return v

def KDJ(data, N1=9, N2=3, N3=3):
    low  = data.low.rolling(N1).min()
    high = data.high.rolling(N1).max()
    rsv  = (data.close - low) / (high - low) * 100
    k = SMA(rsv,N2)
    d = SMA(k, N3)
    j = k * 3 - d * 2
    return k, d, j

def VMA(amount, volume, peried = 5):
    svolume = sum(volume)
    samount = sum(amount)
    return MA(pd.Series(vprice), peried)

def BaseFloatingProfit(df, mdate = None):
    for _index, aprice in df.aprice.iteritems():
        pass

def GameKline(df, dist_data, mdate = None):
    if mdate is None:
        p_low_vol_list = list()
        p_high_vol_list = list()
        p_open_vol_list = list()
        p_close_vol_list = list()
        for _index, date in df.date.iteritems():
            drow = df.loc[_index]
            p_low = drow['low']
            p_high = drow['high']
            p_open = drow['open']
            p_close = drow['close']
            outstanding = drow['outstanding']
            p_low_vol_list.append(dist_data[dist_data.price < p_low].volume.sum() / outstanding)
            p_high_vol_list.append(dist_data[dist_data.price < p_high].volume.sum() / outstanding)
            p_open_vol_list.append(dist_data[dist_data.price < p_open].volume.sum() / outstanding)
            p_close_vol_list.append(dist_data[dist_data.price < p_close].volume.sum() / outstanding)
        df['low_p'] = p_low_vol_list
        df['high_p'] = p_high_vol_list
        df['open_p'] = p_open_vol_list
        df['close_p'] = p_close_vol_list
    else:
        drow = df.loc[df.date == mdate]
        p_low = drow['low']
        p_high = drow['high']
        p_open = drow['open']
        p_close = drow['close']
        outstanding = drow['outstanding']
        df.at[df.date == mdate, 'low_p']   = dist_data[dist_data.price < p_low].volume.sum() / outstanding
        df.at[df.date == mdate, 'high_p']  = dist_data[dist_data.price < p_high].volume.sum() / outstanding
        df.at[df.date == mdate, 'open_p']  = dist_data[dist_data.price < p_open].volume.sum() / outstanding
        df.at[df.date == mdate, 'close_p'] = dist_data[dist_data.price < p_close].volume.sum() / outstanding
    return df
        
#function           : u-limitted t-day moving avering price
#input data columns : ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
def Mac(df, peried = 0):
    ulist = list()
    df = df.sort_values(by = 'date', ascending= True)
    for name, group in df.groupby(df.date):
        if peried != 0 and len(group) > peried:
            group = group.nlargest(peried, 'pos')
        total_volume = group.volume.sum()
        total_amount = group.price.dot(group.volume)
        ulist.append(total_amount / total_volume)
    return ulist

if __name__ == "__main__":
    code = '601318'
    prestr = "1" if get_market_name(code) == "sh" else "0"
    cstock = CStock(code, ct.DB_INFO)
    data = cstock.get_k_data()
    data['close'] = data.amount/data.volume

    info = pd.read_csv("/Volumes/data/quant/stock/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
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
