# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from cindex import CIndex
from cstock import CStock
from datetime import datetime
from cstock_info import CStockInfo
from common import is_df_has_unexpected_data
from pandas.plotting import register_matplotlib_converters
import pprint
import matplotlib
import const as ct
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statsmodels.tsa.stattools as ts
register_matplotlib_converters()
def plot_price_series(df, ts1, ts2):
    months = mdates.MonthLocator()  # every month
    plt.figure(figsize=(12,6))
    plt.plot(df['date'], df[ts1], label=ts1)
    plt.plot(df['date'], df[ts2], label=ts2)
    plt.xlabel('time')
    plt.gca().xaxis.set_major_locator(months)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.ylabel('price')
    plt.xticks(size='small',rotation=68,fontsize=13)
    plt.title('%s and %s Daily Prices' % (ts1, ts2))
    plt.legend()
    plt.show()

def plot_scatter_series(df, ts1, ts2):
    plt.xlabel('%s Price ($)' % ts1)
    plt.ylabel('%s Price ($)' % ts2)
    plt.title('%s and %s Price Scatterplot' % (ts1, ts2))
    plt.scatter(df[ts1], df[ts2])
    plt.show()

def plot_residuals(df):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df['date'], df["res"], label="Residuals")
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.xticks(size='small',rotation=68,fontsize=13)
    plt.title('Residual Plot')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    start_date = '2017-01-01'
    end_date = '2019-09-20'
    REDIS_HOST = '127.0.0.1'
    index_obj = CIndex('000300', dbinfo = ct.OUT_DB_INFO, redis_host = REDIS_HOST)
    index_df = index_obj.get_components_data(cdate = end_date)
    stock_info = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = REDIS_HOST).get()
    code_list = stock_info['code'].tolist()
    name_list = stock_info['name'].tolist()
    code2namedict = dict(zip(code_list, name_list))
    index_codes = index_df.code.tolist()
    for i, scode in enumerate(index_codes):
        for j, tcode in enumerate(index_codes):
            if j <= i: continue
            source = CStock(scode, dbinfo = ct.OUT_DB_INFO, redis_host = REDIS_HOST)
            target = CStock(tcode, dbinfo = ct.OUT_DB_INFO, redis_host = REDIS_HOST)
            source_df = source.get_k_data_in_range(start_date, end_date)
            source_df.date = pd.to_datetime(source_df.date)
            source_df = source_df.set_index('date')
            target_df = target.get_k_data_in_range(start_date, end_date)
            target_df.date = pd.to_datetime(target_df.date)
            target_df = target_df.set_index('date')
            df = pd.DataFrame(index = source_df.index | target_df.index)
            df[scode] = source_df["close"]
            df[tcode] = target_df["close"]
            if is_df_has_unexpected_data(df):
                df = df.dropna()
            #plot_price_series(df, "601318", "601628")
            #plot_scatter_series(df, "601318", "601628")
            ##calculate optimal hedge ratio "beta"
            res = sm.OLS(df[scode], df[tcode]).fit()
            beta_hr = res.params[tcode]
            ##calculate the residuals of the linear combination
            df["res"] = df[scode] - beta_hr*df[tcode]
            ##plot the residuals
            #plot_residuals(df)
            ##calculate and output the CADF test on the residuals
            cadf = ts.adfuller(df["res"])
            if cadf[0] < cadf[4]['1%']:
                print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01")
                print(code2namedict[scode], code2namedict[tcode])
                pprint.pprint(cadf)
                print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA02")
