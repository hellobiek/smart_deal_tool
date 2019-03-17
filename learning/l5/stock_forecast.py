# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker as mticker
from mpl_finance import candlestick2_ochl
from cindex import CIndex
from technical.ad import ad
from datetime import datetime
from technical.kdj import kdj
from technical.roc import roc
from technical.ma import ma, MACD
from alpha_vantage.timeseries import TimeSeries
from sklearn.svm import LinearSVC, SVC
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA
COLORS = ['#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082', '#F5DEB3']
def get_vantage_client():
    VANTAGE_API_KEY = 'R4RZ079WZET1M0JB'
    return TimeSeries(key = VANTAGE_API_KEY, output_format = 'pandas')

def get_index_data(start_date, end_date, index_code):
    iobj = CIndex(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    i_data = iobj.get_k_data_in_range(start_date, end_date)
    i_data = i_data[['open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
    return i_data

def plot(data, keys, ilabel, has_subplot = True):
    fig = plt.figure(figsize=(9,5))
    ax = fig.add_subplot(2, 1, 1)
    ax.set_xticklabels([])
    plt.title('close price - %s chart' % ilabel)
    plt.grid(True)
    candlestick2_ochl(ax, data['open'], data['close'], data['high'], data['low'], width = 1.0, colorup = 'r', colordown = 'g')
    ax.set_ylabel(ilabel)
    ax.set_xticks(range(0, len(data.index), 10))
    if has_subplot:
        bx = fig.add_subplot(2, 1, 2)
    plt.ylabel('%s values' % ilabel)
    i = 0
    for key in keys:
        plt.plot(data[key], COLORS[i], lw = 0.75, linestyle = '-', label = key)
        i += 1
    plt.legend(loc = 2, prop = {'size':9.5})
    plt.grid(True)
    fig.autofmt_xdate()
    plt.show()

def create_data_series(symbol, start_date, end_date):
    #vc = get_vantage_client()
    #ts, meta = vc.get_intraday(symbol, interval='1min', outputsize='full')
    #ts = ts[ts.index.str.startswith('2019-03-14')]
    #ts = ts.rename(columns = {"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close", "5. volume": "volume"})
    #plot(ts, ['k', 'd', 'j'], 'kdj')
    #plot(ts, ['roc', 'roc_ma'], 'roc')
    #plot(ts, ['ad_5', 'ad_10', 'ad_20'], 'ad')
    ts = get_index_data(start_date = start_date, end_date = end_date, index_code = symbol)
    ts['date'] = pd.to_datetime(ts['date'])
    ts.set_index('date', inplace = True)
    ts = ma(ts, 5)
    ts = ma(ts, 10)
    ts = ma(ts, 20)
    ts = ad(ts, 5)
    ts = ad(ts, 10)
    ts = ad(ts, 20)
    ts = kdj(ts)
    ts = roc(ts, 5, 10)
    ts = MACD.macd(ts)
    #create the new lagged dataFrame
    tslag = pd.DataFrame(index = ts.index)
    tslag['k'] = ts['k']
    tslag['d'] = ts['d']
    tslag['j'] = ts['j']
    tslag['macd'] = ts['macd']
    tslag['roc'] = ts['roc'] - ts['roc_ma']
    tslag["ad_quick"]  = ts["ad_5"] - ts["ad_10"]
    tslag["ad_slow"]   = ts["ad_5"] - ts["ad_20"]

    #create the shifted lag series of prior trading period close values
    tslag["ma_5"] = ts["close"] - ts["ma_5"]
    tslag["ma_10"] = ts["close"] - ts["ma_10"]

    tslag["today"] = ts["close"].pct_change() * 100.0
    for i,x in enumerate(tslag["today"]):
        if (abs(x) < 0.001): tslag["today"][i] = 0.0001
    
    tslag['direction'] = np.sign(tslag["today"])

    #create the "direction" column (+1 or -1) indicating an up/down day
    tslag = tslag[(tslag.index >= start_date) & (tslag.index <= end_date)]
    return tslag

if __name__ == "__main__":
    # create a lagged series of the S&P500 US stock market index
    snpret = create_data_series("000300", '2010-01-10', '2017-01-01')
    snpret = snpret.dropna(how = 'any')
    
    # use the prior two days of returns as predictor values, with direction as the response
    X = snpret[["ma_5", "ma_10", "ad_quick", "ad_slow", "roc", "macd"]]
    y = snpret["direction"]

    # the test data is split into two parts: Before and after 1st Jan 2005.
    start_test = datetime.strptime('2016-01-01', '%Y-%m-%d')

    # create training and test sets
    X_train = X[X.index < start_test]
    X_test = X[X.index >= start_test]
    y_train = y[y.index < start_test]
    y_test = y[y.index >= start_test]
  
    # create the (parametrised) models
    print("hit rates/confusion matrices:\n")
    models = [("LR", LogisticRegression(solver = 'lbfgs')), ("LDA", LDA()), ("QDA", QDA()), ("LSVC", LinearSVC()),
              ("RSVM", SVC(
              	C=1000000.0, cache_size=200, class_weight=None,
                coef0=0.0, degree=3, gamma=0.0001, kernel='rbf',
                max_iter=-1, probability=False, random_state=None,
                shrinking=True, tol=0.001, verbose=False)
              ),
              ("RF", RandomForestClassifier(
              	n_estimators=1000, criterion='gini', 
                max_depth=None, min_samples_split=2, 
                min_samples_leaf=1, max_features='auto', 
                bootstrap=True, oob_score=False, n_jobs=1, 
                random_state=None, verbose=0)
              )]

    # iterate through the models
    for m in models:
        # train each of the models on the training set
        m[1].fit(X_train, y_train)
        # make an array of predictions on the test set
        pred = m[1].predict(X_test)
        # output the hit-rate and the confusion matrix for each model
        print("%s:\n%0.3f" % (m[0], m[1].score(X_test, y_test)))
        print("%s\n" % confusion_matrix(pred, y_test))
