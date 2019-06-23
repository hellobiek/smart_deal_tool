#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import operator
import numpy as np
import pandas as pd
import tushare as ts
import statsmodels.api as sm
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from ccalendar import CCalendar
import statsmodels.tsa.stattools as sts
from sklearn.model_selection import train_test_split
from base.cdate import transfer_int_to_date_string, get_dates_array
from common import get_tushare_client, add_suffix, create_redis_obj
fpath     = '/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/tushare.json' 
mredis    = create_redis_obj(host = "127.0.0.1")
ts_client = get_tushare_client(fpath)

def select_code(code_list, start_date, end_date):
    date_arrays = list()
    for mdate in get_dates_array(start_date, end_date, dformat = "%Y%m%d"):
        if CCalendar.is_trading_day(transfer_int_to_date_string(mdate), redis = mredis):
            date_arrays.append(mdate)
    #choose stock which is not suspended verry long
    total_df = pd.DataFrame()
    for code in code_list:
        df = ts.pro_bar(pro_api = ts_client, ts_code = add_suffix(code), adj = 'qfq', start_date = start_date, end_date = end_date)
        if df is None: continue
        if len(df) > int(0.8 * len(date_arrays)):
            df = df.rename(columns = {"ts_code": "code", "trade_date": "date", "pct_change": "pchange"})
            df = df.set_index('date')
            total_df[code] = df.close
    return total_df

def compute_cointegration_pairs(df):
    rank      = dict()
    visited   = list()
    code_list = df.columns.tolist()
    for code_i in code_list:
        for code_j in code_list:
            if code_j != code_i and code_j not in visited:
                tmp_df = pd.concat([df[code_i], df[code_j]], axis = 1, sort = False)
                tmp_df = tmp_df.dropna()
                if len(tmp_df) > 0:
                    results = sm.OLS(tmp_df[code_j], tmp_df[code_i]).fit()
                    beta    = results.params.tolist()
                    spread  = tmp_df[code_j] - beta * tmp_df[code_i]
                    spread  = spread.dropna()
                    sta     = sts.adfuller(spread)
                    if sta[0] < sta[4]['1%'] and sta[1] < 0.001:
                        #print(code_i, code_j, beta, sta)
                        rank[code_i + '_' + code_j] = (np.std(spread, ddof = 1), np.mean(spread), beta)
        visited.append(code_i)
    pairs = sorted(rank.items(), key = operator.itemgetter(1), reverse = True)
    if len(pairs) > 0:
        return pairs[0]

def plot(pair, df):
    (xcode, ycode) = pair[0].split('_')
    (std, mean, beta) = pair[1]
    tmp_df = pd.concat([df[xcode], df[ycode]], axis = 1, sort = False)
    tmp_df = tmp_df.dropna()
    tmp_df = tmp_df.reset_index(drop = True)
    average_mean  = np.mean(tmp_df[ycode] - tmp_df[xcode])
    delta = tmp_df[ycode] - beta * tmp_df[xcode]

    plt.plot(tmp_df.index.tolist(), tmp_df[xcode], label = '%s' % xcode)
    plt.plot(tmp_df.index.tolist(), tmp_df[ycode] - average_mean, label = '%s' % ycode)
    plt.xlabel('time')
    plt.ylabel('price')
    plt.title('price and time between %s and %s' % (xcode, ycode))
    plt.legend()
    plt.show()
    #####################
    plt.plot(tmp_df.index.tolist(), (delta - mean)/std, label = 'delta price')
    plt.show()
    plt.xlabel('hist')
    plt.ylabel('value')
    plt.title('price and time between %s and %s' % (xcode, ycode))
    plt.hist((delta - mean)/std, 60, histtype = 'bar', rwidth = 0.6)
    plt.show()
    print("xcode:%s, ycode:%s, mean:%s, std:%s, beta:%s" % (xcode, ycode, mean, std, beta))
        
def main(code_list, start_date, end_date):
    df    = select_code(code_list, start_date, end_date)
    pair = compute_cointegration_pairs(df)
    if pair is not None:
        plot(pair, df)

if __name__ == '__main__':
    start_date = '20170214'
    end_date   = '20180214'
    code_list  = ['002079', '002119', '002129', '002156', '002185', '002218', '002449', '002638', '002654', '002724', '002745', '002815', '002913', '300046', '300053', '300077', '300080', '300102', '300111', '300118', '300223', '300232', '300236', '300241', '300269', '300296', '300301', '300303', '300317', '300323', '300327', '300373', '300389', '300582', '300613', '300623', '300625', '300632', '300671', '300672', '300708', '600151', '600171', '600360', '600460', '600206', '600537', '600584', '600667', '600703', '601012', '603005', '603501', '603986', '300749']
    main(code_list, start_date, end_date)
