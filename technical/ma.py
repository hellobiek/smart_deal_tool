#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
def ma(df, n):
    """calculate the moving average for the given data.
    :param df: pandas.DataFrame
    :param n:
    :return: pandas.DataFrame
    """
    ma = pd.Series(df['close'].rolling(n, min_periods=n).mean(), name = 'ma_' + str(n))
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

class MACD:
    @staticmethod
    def macd(data, nfast = 12, nslow = 26, mid = 9):
        data = ewma(data, nslow)
        data = ewma(data, nfast)
        dif = pd.Series(data['ewma_%s' % nfast] - data['ewma_%s' % nslow], name = 'dif')
        dea = pd.Series(dif.ewm(span = mid, min_periods = mid).mean(), name = 'dea')
        macd = pd.Series((dif - dea) * 2, name = 'macd')
        data = data.join(dif) 
        data = data.join(dea) 
        data = data.join(macd) 
        return data

    @staticmethod
    def is_cross_points_dist_far(gold_indexs, dead_indexs, index, delta = 10):
        # make sure gold cross isn't too close to death cross
        for i in range(len(gold_indexs)):
            if abs(gold_indexs[i] - index) <= delta: return False
        for i in range(len(dead_indexs)):
            if abs(dead_indexs[i] - index) <= delta: return False
        return True

    @staticmethod
    def get_two_cross_point_indexs_for_bottom_divergence(data, index, delta = 7):
        indexs_tuple_list = list()
        for i in range(index, -len(data), -1):
            if MACD.is_gold_cross(data, i):
                for j in range(i - 1, -len(data), -1):
                    if MACD.is_gold_cross(data, j):
                        i = j + 1
                        break
                    if MACD.is_dead_cross(data, j):
                        if abs(j - i) > delta: 
                            indexs_tuple_list.append((j, i))
                        break
            if len(indexs_tuple_list) == 2:
                return indexs_tuple_list
        else:
            return list()

    @staticmethod
    def get_two_cross_point_indexs_for_top_divergence(data, index, delta = 7):
        indexs_tuple_list = list()
        for i in range(index, -len(data), -1):
            if MACD.is_dead_cross(data, i):
                for j in range(i - 1, -len(data), -1):
                    if MACD.is_dead_cross(data, j):
                        i = j + 1
                        break
                    if MACD.is_gold_cross(data, j):
                        if abs(j - i) > delta: 
                            indexs_tuple_list.append((j, i))
                        break
            if len(indexs_tuple_list) == 2:
                return indexs_tuple_list
        else:
            return list()

    @staticmethod
    def is_top_divergence(data, index):
        # 判断顶背离
        # if macd > 0, bottom divergence is not right
        if data['macd'].iloc[index] > 0: return False
        # get last 2 gold cross index and dead cross index
        indexs_list = MACD.get_two_cross_point_indexs_for_top_divergence(data, index)
        if 0 == len(indexs_list): return False

        # calculate current negative bar area
        latest_data = data['macd'][indexs_list[0][0]:indexs_list[0][1]]
        latest_area = abs(sum(latest_data))
        latest_prices = data['high'][indexs_list[0][0]:indexs_list[0][1]]
        latest_highest_price = max(latest_prices)

        # calculate pre negative bar area
        pre_data = data['macd'][indexs_list[1][0]:indexs_list[1][1]]
        pre_area = abs(sum(pre_data))
        pre_prices = data['high'][indexs_list[1][0]:indexs_list[1][1]]
        pre_highest_price = max(pre_prices)
        return True if latest_area < pre_area and latest_highest_price > pre_highest_price else False

    @staticmethod
    def is_bottom_divergence(data, index):
        # 判断底背离
        # if macd > 0, bottom divergence is not right
        if data['macd'].iloc[index] < 0: return False
        # get last 2 gold cross index and dead cross index
        indexs_list = MACD.get_two_cross_point_indexs_for_bottom_divergence(data, index)
        if 0 == len(indexs_list): return False

        # calculate current negative bar area
        latest_data = data['macd'][indexs_list[0][0]:indexs_list[0][1]]
        latest_area = abs(sum(latest_data))
        latest_prices = data['low'][indexs_list[0][0]:indexs_list[0][1]]
        latest_lowest_price = min(latest_prices)

        # calculate pre negative bar area
        pre_data = data['macd'][indexs_list[1][0]:indexs_list[1][1]]
        pre_area = abs(sum(pre_data))
        pre_prices = data['low'][indexs_list[1][0]:indexs_list[1][1]]
        pre_lowest_price = min(pre_prices)
        return True if latest_area < pre_area and latest_lowest_price < pre_lowest_price else False

    @staticmethod
    def is_macd_down(data, dtype = 'macd',  n = 3):
        # macd柱线递减
        x = data[dtype]
        for i in range(1, n):
            if x.iloc[-i] >= x.iloc[-(i + 1)]: return False
        return True

    @staticmethod
    def is_macd_up(data, dtype = 'macd', n = 3):
        # macd柱线递增
        x = data[dtype]
        for i in range(1, n):
            if x.iloc[-i] <= x.iloc[-(i + 1)]: return False
        return True

    @staticmethod
    def is_dead_cross(data, i = 1):
        # 死叉: 当DIF线下穿DEA线时,这种技术形态叫做MACD死叉,
        # 1. 前一天macd >= 0,今天macd < 0
        macd = data['macd']
        return True if macd.iloc[i] < 0 and macd.iloc[i - 1] >= 0 else False

    @staticmethod
    def is_gold_cross(data, i = -1):
        # 黄金叉: 当DIF线上穿DEA线时,这种技术形态叫做MACD金叉,
        # 1. 前一天macd <= 0,今天macd > 0  2. macd指标在0轴上方（此条件暂时忽略）
        macd = data['macd']
        return True if macd.iloc[i] > 0 and macd.iloc[i - 1] <= 0 else False
