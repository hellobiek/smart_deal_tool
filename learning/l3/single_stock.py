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
from hurst import hurst
 
def plot_price_series(df, ts1):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df[ts1], label=ts1)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(datetime(2004, 3, 12), datetime(2018, 6, 28))
    ax.grid(True)
    fig.autofmt_xdate()
 
    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('%s Daily Prices' % (ts1))
    plt.legend()
    plt.savefig('images/%s.png' % ts1, dpi=1000)
 
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

def get_name(code_id):
    redis = create_redis_obj()
    df_byte = redis.get("stockInfo")
    df = _pickle.loads(df_byte)
    df = df[['code', 'name']]
    return df.loc[df.code == code_id]['name'].values[0]

if __name__ == "__main__":
    code_list = ['002376', '002821', '603025', '603019', '300678', '300650', '002766', '600841', '300447', '300692',\
                 '002773', '300651', '002798', '603811', '603030', '002161', '300257', '603797', '300082', '603813',\
                 '300337', '300445', '300444', '300685', '002770', '603027', '300732', '002358', '300722', '002833',\
                 '300736', '000759', '603989', '300642', '603368', '300482', '300441', '300455', '603630', '002775',\
                 '002761', '002749', '300680', '603787', '300286', '603222', '002603', '300655', '300682', '300457',\
                 '300495', '002038', '300668', '300654', '300640', '603021', '300708', '300720', '603960', '603579',\
                 '300579', '603085', '002842', '002881', '002895', '603683', '002922', '603898', '603126', '300431',\
                 '002513', '603318', '300395', '300424', '002738', '600406', '600348', '603655', '002923', '300626',\
                 '603696', '600228', '603912', '300552', '002841', '603737', '002896', '600389', '002921', '300396',\
                 '601069', '300625', '603871', '300631', '002908', '002883', '002461', '002878', '002893', '603040',\
                 '300621', '603108', '300437', '300423', '300422', '603690', '002919', '300620', '002886', '603900',\
                 '002851', '300740', '300595', '603533', '603080', '603679', '300434', '603335', '002264', '300390',\
                 '002715', '603678', '300623', '603283', '300558', '603517', '002863', '600234', '002917', '600354',\
                 '300438', '300388', '300439', '002725', '603660', '300607', '002916', '603728', '603066', '603516',\
                 '002492', '603138', '300406', '002732', '603688', '002915', '002901', '300638', '002336', '600959',\
                 '603505', '300589', '300012', '002859', '002905', '300628', '603882', '002079', '300629', '002910',\
                 '600390', '300577', '603289', '603260', '603937', '300603', '002735', '603499', '300398', '603466',\
                 '603315', '603329', '300400', '603881', '603895', '300602', '002873', '300711', '300659', '300665',\
                 '300103', '300671', '603825', '603616', '002753', '002747', '600323', '300498', '603429', '600691',\
                 '002752', '300664', '300102', '603818', '603011', '603788', '300706', '300712', '002803', '300128',\
                 '603601', '300470', '600123', '603416', '300698', '600080', '603599', '603228', '300298', '002423',\
                 '603566', '300717', '000793', '300461', '601211', '300448', '300662', '300716', '000553', '603567',\
                 '002378', '603229', '000584', '002391', '603203', '300700', '603968', '603997', '603808', '002756',\
                 '000633', '002024', '300462', '603363', '300339', '300477', '603160', '002838', '002390', '000587']

    for code_id in code_list:
        if code_id in ['601990', '603587', '603666', '601066']: continue
        tmp_df = df[code_id]
        tmp_df = tmp_df.dropna(axis = 0, how = 'all')
        res = hurst(tmp_df)
        if res > 0.5:
            name = get_name(code_id)
            print("code_id:%s, name:%s hurst:%s" % (code_id, name, res))
