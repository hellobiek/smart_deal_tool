# coding=utf-8
import os
import _pickle 
import pprint
import datetime
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statsmodels.tsa.stattools as ts
from pandas.stats.api import ols
 
def plot_price_series(df, ts1, ts2):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df[ts1], label=ts1)
    ax.plot(df.index, df[ts2], label=ts2)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(datetime(2004, 3, 12), datetime(2018, 6, 28))
    ax.grid(True)
    fig.autofmt_xdate()
 
    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('%s and %s Daily Prices' % (ts1, ts2))
    plt.legend()
    plt.savefig('images/plot_price_series.png', dpi=1000)
 
def plot_scatter_series(df, ts1, ts2):
    plt.xlabel('%s Price ($)' % ts1)
    plt.ylabel('%s Price ($)' % ts2)
    plt.title('%s and %s Price Scatterplot' % (ts1, ts2))
    plt.scatter(df[ts1], df[ts2])
    plt.savefig('images/plot_scatter_series.png', dpi=1000)
 
def plot_residuals(df):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df, label="Residuals")
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(datetime(2014, 3, 12), datetime(2018, 6, 28))
    ax.grid(True)
    fig.autofmt_xdate()
 
    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('Residual Plot')
    plt.legend()
 
    plt.plot(df)
    plt.savefig('images/residuals.png', dpi=1000)

def dateRange(beginDate, endDate):
    date_l=[datetime.strftime(x,'%Y-%m-%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

min_date = datetime(2999, 1, 1)
max_date = datetime(2000, 1, 1)

def gen_date_df():
    global min_date
    global max_date
    df = pd.DataFrame()
    list_files = os.listdir("data")
    for t_file in list_files:
        x = pd.read_csv("data/%s" % t_file, header = None)
        x.columns = ['date','open','close','high','low','volume','amount']
        _last = len(x) - 1
        _start = datetime.strptime(x.loc[0]['date'], "%Y-%m-%d")
        min_date = min(min_date, _start)
        _end = datetime.strptime(x.loc[_last]['date'], "%Y-%m-%d")
        max_date = max(max_date, _end)
    date_list = dateRange(min_date, max_date)
    df['date'] = date_list
    return df
    
def stock_csv():
    df = gen_date_df()
    list_files = os.listdir("data")
    for t_file in list_files:
        code = t_file.split('.')[0]
        df[code] = np.nan
        _tmp = pd.read_csv("data/%s" % t_file, header = None)
        _tmp.columns = ['date','open','close','high','low','volume','amount']
        for _index, _date in _tmp.date.items():
            cvalue = _tmp.loc[_index].close
            vindex = df[df.date == _date].index.tolist()[0]
            print("vindex:%s, code:%s, value:%s" % (vindex, code, cvalue))
            df.set_value(vindex, code, cvalue)
    return df

def create_redis_obj(host = 'redis-container', port = 6379, decode_responses = False):
    import redis
    pool = redis.ConnectionPool(host = host, port = port, decode_responses = decode_responses)
    return redis.StrictRedis(connection_pool=pool)

## Plot the residuals
#plot_residuals(tmp_df)
## Plot the two time series
#plot_price_series(df, '002179', '600535')
## Display a scatter plot of the two time series
#plot_scatter_series(df, '002179', '600535')
if __name__ == "__main__":
    #df = stock_csv()
    #redis.set("TestDf", _pickle.dumps(df, 2))
    redis = create_redis_obj()
    df_byte = redis.get("TestDf")
    df = _pickle.loads(df_byte)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df.dropna(axis = 0, how = 'all')

    rdf = pd.DataFrame(columns=["source", "target", "C0", "C1", "B1", "B5", "B10", "dates"])
    for source_code in df.columns:
        if source_code in ['601990', '603587', '603666', '601066']: continue
        for target_code in df.columns:
            if target_code in ['601990', '603587', '603666', '601066']: continue
            if target_code == source_code: continue
            #print("start source:%s slen:%s target:%s tlen:%s" % (source_code, len(df[source_code]), target_code, len(df[target_code])))

            source_df = df[source_code].dropna(axis = 0, how = 'all')
            target_df = df[target_code].dropna(axis = 0, how = 'all')

            source_date_set = set(source_df.index)
            target_date_set = set(target_df.index)
            x = list(source_date_set & target_date_set)

            if len(x) < 60: continue

            source_df = source_df[source_df.index.isin(x)]
            target_df = target_df[target_df.index.isin(x)]

            # Calculate optimal hedge ratio "beta"
            res = ols(y = source_df, x = target_df)
            beta_hr = res.beta.x
 
            # Calculate the residuals of the linear combination
            tmp_df = source_df - beta_hr*target_df
            tmp_df = tmp_df.dropna(axis = 0, how = 'all')
 
            # Calculate and output the CADF test on the residuals
            cadf = ts.adfuller(tmp_df)
            if cadf[0] < cadf[4]['5%'] and cadf[0] < cadf[4]['1%'] and cadf[0] < cadf[4]['10%']:
                rdf.append({"source":source_code, "target":target_code, "C0":cadf[0], "C1":cadf[1], "B1":cadf[4]['1%'], "B5":cadf[4]['5%'], "B10":cadf[4]['10%'], "dates":x}, ignore_index=True)
    redis.set("rdict", _pickle.dumps(rdf, 2))
