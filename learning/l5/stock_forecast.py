# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import pydotplus
import const as ct
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker as mticker
from mpl_finance import candlestick2_ochl
from cindex import CIndex
from cstock import CStock
from datetime import datetime
from algotrade.technical.ad import ad
from algotrade.technical.kdj import kdj
from algotrade.technical.roc import roc
from algotrade.technical.toc import toc
from algotrade.technical.atr import atr
from algotrade.technical.boll import boll
from algotrade.technical.ma import ma, macd
from sklearn.svm import LinearSVC, SVC
from sklearn.tree import export_graphviz
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA
COLORS = ['#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082', '#F5DEB3']
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
    ts = get_index_data(start_date = start_date, end_date = end_date, index_code = symbol)
    ts['date'] = pd.to_datetime(ts['date'])
    ts.set_index('date', inplace = True)
    ts = ma(ts, 5)
    ts = ma(ts, 10)
    ts = ma(ts, 20)
    ts = ma(ts, 60)
    ts = ad(ts, 5)
    ts = ad(ts, 10)
    ts = ad(ts, 20)
    ts = kdj(ts)
    ts = roc(ts, 5, 10)
    ts = macd(ts)
    #create the new lagged dataFrame
    tslag = pd.DataFrame(index = ts.index)
    tslag['k'] = ts['k']
    tslag['d'] = ts['d']
    tslag['macd'] = ts['macd']
    tslag['roc'] = ts['roc'] - ts['roc_ma']

    #create the shifted lag series of prior trading period close values

    for i in range(5):
        tslag["pre_%s" % str(i+1)] = 100 * ts["close"].pct_change(i+1)
    tslag["ma_5"] = (100 * (ts["close"] - ts["ma_5"])) / ts["close"]
    tslag["ma_10"] = (100 * (ts["close"] - ts["ma_10"])) / ts["close"]
    tslag["ma_20"] = (100 * (ts["close"] - ts["ma_20"])) / ts["close"]

    tslag["today"] = ts["close"].pct_change() * 100.0
    for i,x in enumerate(tslag["today"]):
        if (abs(x) < 0.001): tslag["today"][i] = 0.0001
    
    tslag['direction'] = np.sign(tslag["today"])

    #create the "direction" column (+1 or -1) indicating an up/down day
    tslag = tslag[(tslag.index >= start_date) & (tslag.index <= end_date)]
    return tslag

def get_max_profit_min_loss(data, mdate, n, key = 'tchange'):
    start_index = data.loc[data.date == mdate].index.values[0]
    end_index = start_index + n - 1
    profit = data.loc[start_index:end_index, key].max()
    loss = data.loc[start_index:end_index, key].min()
    if np.isnan(profit) or np.isnan(loss): return -1
    return 1 if profit >= 5 and loss >= -3 else -1

def generate_data(code, start_date, end_date, feature_list):
    sobj = CStock(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = sobj.get_k_data()
    data = ma(data, 5)
    data = ma(data, 10)
    data = ma(data, 20)
    data = ma(data, 60)
    data = ma(data, 10, key = 'volume', name = 'volume')
    data = kdj(data)
    data = boll(data)
    data = atr(data, 5)
    data = roc(data, 5, 10)
    data = toc(data, 5, 10)
    data['gamekline'] = (data['pchange'] + data['gamekline']) / (data['turnover'] + abs(data['profit']))
    data['ppercent'] = data['ppercent'] / data['turnover']
    data['hlzh'] = data['ppercent'] - data['npercent']
    data['atr'] = 100 * data['atr'] / data['close']
    data['roc'] = 100 * data['roc_ma']
    data['toc'] = 100 * data['toc_ma']
    data['boll'] = (100 * (data['close'] - data['mb'])) / data['close']
    data["ma_5"] = (100 * (data["close"] - data["ma_5"])) / data["close"]
    data["ma_10"] = (100 * (data["close"] - data["ma_10"])) / data["close"]
    data["ma_20"] = (100 * (data["close"] - data["ma_20"])) / data["close"]
    data["ma_60"] = (100 * (data["close"] - data["ma_60"])) / data["close"]
    data["uprice"] = (100 * (data["ma_60"] - data["uprice"])) / data["close"]
    data = data.reset_index(drop = True)
    data["tchange"] = data["close"].pct_change(20) * 100.0
    data['direction'] = data.apply(lambda row: get_max_profit_min_loss(data, row['date'], 20), axis = 1)
    data = data[(data.date >= start_date) & (data.date <= end_date)]
    data = data.sort_values(by=['date'], ascending = True)
    data['date'] = pd.to_datetime(data['date'])
    data = data.set_index('date')
    data = data.dropna(how='any')
    tslag = pd.DataFrame(index = data.index)
    tslag = data[feature_list]
    tslag['direction'] = data['direction']
    #create the "direction" column (+1 or -1) indicating an up/down day
    tslag = tslag[(tslag.index >= start_date) & (tslag.index <= end_date)]
    tslag = tslag.dropna(how='any')
    return tslag

def create_model():
    redis_host = "127.0.0.1"
    dbinfo = ct.OUT_DB_INFO
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    sci_val_file_path = "/Volumes/data/quant/crawler/china_security_industry_valuation/stock" 
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    ftm = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo = dbinfo, redis_host = redis_host, should_create_mysqldb = True)
    return ftm

if __name__ == "__main__":
    feature_list = ['k', 'd', 'atr', 'hlzh', 'gamekline', 'ppercent', 'ma_5', 'ma_10', 'ma_20', 'ma_60', 'uprice', 'profit', 'boll', 'roc_ma', 'toc_ma']
    snpret = generate_data('600323', '2006-01-01', '2019-11-05', feature_list)
    # use the prior two days of returns as predictor values, with direction as the response
    X = snpret[feature_list]
    y = snpret["direction"]

    # the test data is split into two parts: Before and after 1st Jan 2005.
    start_test = datetime.strptime('2015-01-01', '%Y-%m-%d')

    # create training and test sets
    X_train = X[X.index < start_test]
    X_test = X[X.index >= start_test]
    y_train = y[y.index < start_test]
    y_test = y[y.index >= start_test]
  
    # create the (parametrised) models
    print("hit rates/confusion matrices:\n")
    model = RandomForestClassifier(n_estimators=3000, criterion='entropy', max_depth=None, max_features='auto',
                                   min_samples_split=2, min_samples_leaf=1, bootstrap=True, oob_score=False,
                                   n_jobs=1, random_state=None, verbose=0)
    # train each of the models on the training set
    model.fit(X_train, y_train)
    # make an array of predictions on the test set
    y_pred = model.predict(X_test)
    # output the hit-rate and the confusion matrix for each model
    print("%f\n" % model.score(X_test, y_test))
    print("%s\n" % confusion_matrix(y_test, y_pred, labels = [1, -1]))
    ## store 输出为pdf格式
    #dot_data = export_graphviz(model.estimators_[0], out_file=None, feature_names=feature_list, class_names=['right', 'wrong'], filled=True, rounded=True, special_characters=True)
    #graph = pydotplus.graph_from_dot_data(dot_data)
    #with open('/Users/hellobiek/Desktop/iris.png', 'wb') as f:
    #    f.write(graph.create_png())
    # show importance
    importances = model.feature_importances_
    std = np.std([tree.feature_importances_ for tree in model.estimators_], axis=0)
    indices = np.argsort(importances)[::-1]
    # print the feature ranking
    print("Feature ranking:")
    for f in range(X_train.shape[1]):
        print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))
    # plot the feature importances of the forest
    plt.figure()
    plt.title("Feature importances")
    plt.bar(range(X_train.shape[1]), importances[indices], color="r", yerr=std[indices], align="center")
    plt.xticks(range(X_train.shape[1]), indices)
    plt.xlim([-1, X_train.shape[1]])
    plt.show()
