#-*- coding: utf-8 -*-
#http://mariofilho.com/can-machine-learning-model-predict-the-sp500-by-looking-at-candlesticks/
import quandl
import talib as ta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
quandl.ApiConfig.api_key = 'oo_pjaFaPvQ2m2jb1T8t'
def get_data(selected = ['KO']):
    data = quandl.get_table('WIKI/PRICES', ticker = selected,\
           qopts = {'columns': ['date', 'high', 'low', 'open', 'close', 'volume', 'adj_close']},\
           date = {'gte': '2014-1-1', 'lte': '2016-12-31'}, paginate=True)
    data['return_t+1'] = data['adj_close'].pct_change(1).shift(-1) 
    data['return_t+3'] = data['adj_close'].pct_change(3).shift(-3) 
    data = data.iloc[:-5]
    data.index = data.date
    return data

def get_candle_features(df, target = 'return_t+1', remove_zero_days = True):
    cdl_methods = [m for m in dir(ta) if 'CDL' in m]
    df_cdl = pd.DataFrame(index = df.index)
    for mtd in cdl_methods:
        df_cdl[mtd] = getattr(ta, mtd)(df['open'], df['high'], df['low'], df['close'])
    tgt = df[target]
    if remove_zero_days:
        non_zero = df_cdl.sum(axis=1) > 0
        tgt = tgt[non_zero]
        df_cdl = df_cdl[non_zero]
    return df_cdl, tgt

def rmse(ytrue, ypred):
    return np.sqrt(mean_squared_error(ytrue, ypred))

def plot_res(ytrue, base_zero, base_avg, pred, name):
    r1,r2,r3 = rmse(ytrue, base_zero), rmse(ytrue, base_avg), rmse(ytrue, pred)
    r2 = r2 - r1
    r3 = r3 - r1
    name = "Difference from zero baseline - {}".format(name)
    fig = pd.Series([0,r2,r3], index=['Zero', 'Train average', 'Random Forest']).plot.bar(title=name)
    plt.tight_layout()
    plt.savefig("/Users/hellobiek/Desktop/%s" % name)

data = get_data()
xtrain, ytrain = get_candle_features(data)
xval, yval = get_candle_features(data)

base_avg = np.ones(yval.shape) * ytrain.mean()
base_zero = np.zeros(yval.shape)

mdl = RandomForestRegressor(n_estimators = 10, n_jobs = 1)
mdl.fit(xtrain, ytrain)
res = mdl.predict(xval)
plot_res(yval, base_zero, base_avg, res, "T+1")
