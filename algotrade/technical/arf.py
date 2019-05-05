#-*- coding: utf-8 -*-
import numpy as np
import pandas as pd
def arf(data, risk_window = 60, risk_diff = 30, sigma_window = 60):
    #parameter:
    #       sigma_window：计算头寸需要用到的数据的数量
    #       risk_diff：risk_adjust_factor 用到的两个sigma间隔大小
    #       risk_window：计算risk_adjust_factor用到的sigma的窗口大小
    #return:
    #       adjust risk factor
    count = max(sigma_window, risk_window + 2 * risk_diff)
    arf   = (data['high'] + data['low'] + 2 * data['close']) / 4
    sigma_list = list()
    risk_adjust_factor_list = list()
    for row in range(len(arf)):
        xslice = arf[:row + 1]
        first_sigma  = np.std(xslice[-risk_window-(risk_diff*2):-(risk_diff*2)])    # -120:-60
        center_sigma = np.std(xslice[-risk_window-(risk_diff*1):-(risk_diff*1)])    #  -90:-30
        last_sigma   = np.std(xslice[-risk_window:])                                #  -60:
        sigma        = np.std(xslice[-sigma_window:])
        risk_adjust_factor_ = 0
        if not np.isnan(first_sigma) and not np.isnan(center_sigma) and not np.isnan(last_sigma):
            if last_sigma > center_sigma:
                risk_adjust_factor_ = 0.5
            elif last_sigma < center_sigma and last_sigma > first_sigma:
                risk_adjust_factor_ = 1.0
            elif last_sigma < center_sigma and last_sigma < first_sigma:
                risk_adjust_factor_ = 1.5
        sigma_list.append(sigma)
        risk_adjust_factor_list.append(risk_adjust_factor_)

    sigma = pd.Series(sigma_list, name = 'sigma', index = data.index)
    arf = pd.Series(risk_adjust_factor_list, name = 'arf', index = data.index)
    
    data = data.join(arf)
    data = data.join(sigma)
    return data
