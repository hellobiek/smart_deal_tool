# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
def is_rolling_peak(stock_data, low_limit, high_limit, direction = ct.ROLLING_UP, peried = None):
    '''
        check if stock is rolling in direction(ROLLING_UP or ROLLING_DOWN) in past peried(day)
        input:
            stock data: dataframe for stock info
            low  limit: low limit for price change relative to uprice
            high limit: high limit for price change relative to uprice
        output:
            True or False
    '''
    return True 
