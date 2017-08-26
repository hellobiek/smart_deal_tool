# -*- coding:utf-8 -*-
import sys,time,datetime,json
import tushare as ts
import pandas as pd
import numpy as np
from mysql import get,get_hist_data,set
from common import _fprint,get_all_tables
from sqlalchemy import create_engine
from pandas import Series,DataFrame
from datetime import datetime,timedelta
from const import MARKET_SH,MARKET_SZ,MARKET_CYB,MARKET_ALL,DB_NAME,DB_USER,DB_PASSWD,DB_HOSTNAME,SQL,SLEEP_INTERVAL
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('max_rows', 200)

def get_classified_stocks(type_name = MARKET_ALL):
    """
        获取股票类型
        Return
        --------
        DataFrame
        code :股票代码
        name :股票名称
    """
    table = "info"
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    df = get(engine, SQL % table)
    df = df[['code', 'name', 'timeToMarket', 'c_name', 'totals', 'outstanding', 'industry', 'area']]
    if type_name == MARKET_SH:
        df = df.ix[df.code.str[0] == '6']
    elif type_name == MARKET_CYB: 
        df = df.ix[df.code.str[0] == '3']
    elif type_name == MARKET_SZ:
        df = df.ix[df.code.str[0] == '0']
    else:
        pass
    return df.sort_values('code').reset_index(drop=True)

def get_concepts(type_name):
    """
        获取题材类型
        Return
        --------
        DataFrame
        code :股票代码
        name :股票名称
    """
    table = "concept"
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    df = get(engine, SQL % table)
    return df.sort_values('code').reset_index(drop=True)

def is_sub_new_stock(time2Market, timeLimit = 365):
    if time2Market == '0': #for stock has not benn in market
        return False
    if time2Market:
       t = time.strptime(time2Market, "%Y%m%d")
       y,m,d = t[0:3]
       time2Market = datetime(y,m,d)
       if (datetime.today()-time2Market).days < timeLimit:
           return True
    return False

def is_code_exists(code_id):
    table = "info"
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    stock_info = get(engine, SQL % table)
    stock_info = stock_info[['code','timeToMarket']]
    if len(stock_info[stock_info.code == code_id].index.tolist()) > 0:
        return True
    return False

def is_after_release(code_id, _date):
    table = "info"
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    stock_info = get(engine, SQL % table)
    stock_info = stock_info[['code','timeToMarket']]
    _index = stock_info[stock_info.code == code_id].index.tolist()[0]
    time2Market = stock_info.loc[_index, 'timeToMarket']
    if time2Market:
        t = time.strptime(str(time2Market), "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)
        if (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0:
            return True
    return False

def is_trading_day(_date):
    table = "calendar"
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    stock_dates_df = get(engine, SQL % table)
    return stock_dates_df.query('calendarDate=="%s"' % _date).isOpen.values[0] == 1

def get_stock_volumes(market, data_times):
    stock_id_frame = get_classified_stocks(market)
    stock_ids = list()
    stock_names = list()
    stock_turnover = list()
    stock_volume = list()
    stock_concepts = list()
    stock_dates = list()
    stock_pchanges = list()
    pydate_array = data_times.to_pydatetime()
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    for _date in date_only_array:
        if is_trading_day(_date):
            for code_index, code_id in stock_id_frame['code'].iteritems():
                if not is_sub_new_stock(str(stock_id_frame['timeToMarket'][code_index])):
                    turnover = 0
                    volume = 0
                    stock_name = stock_id_frame['name'][code_index]
                    stock_concept = stock_id_frame['c_name'][code_index]
                    hist_data = get_hist_data(engine, code_id, _date)
                    if hist_data is not None:
                        hist_data = hist_data[hist_data['volume'] > 0]
                        hist_data = hist_data[['turnover','volume','p_change']]
                        if not hist_data.empty:
                            turnover = hist_data['turnover'][0]
                            p_change = hist_data['p_change'][0]
                            if turnover > 5 and p_change > 5:
                                stock_dates.append(_date)
                                stock_ids.append(code_id)
                                stock_names.append(stock_name)
                                volume = hist_data['volume'][0]
                                stock_volume.append(volume)
                                stock_turnover.append(turnover)
                                stock_concepts.append(stock_concept)
                                stock_pchanges.append(p_change)
    df = DataFrame({'date':stock_dates,'code':stock_ids,'name':stock_names,'turnover':stock_turnover, 'volume': stock_volume,'c_name': stock_concepts,'p_change': stock_pchanges})
    table = 'turnover'
    sql = 'create table if not exists %s(date varchar(10),code varchar(10),name varchar(20),volume float,p_change float,c_name varchar(5000))' % table
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    set(engine,df,table)
    return get(engine, SQL % table)

def get_pre_trading_day(_date):
    table = 'calendar'
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    df = get(engine, SQL % table)
    _index = df[df.calendarDate == _date].index.tolist()[0]
    if _index > 0:
        _tindex = _index
        while _tindex > 0:
            _tindex -= 1
            if df['isOpen'][_tindex] == 1:
                return df['calendarDate'][_tindex]
    raise Exception("can not find pre trading day.")

def get_post_trading_day(_date):
    table = 'calendar'
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    df = get(engine, SQL % table)
    _index = df[df.calendarDate == _date].index.tolist()[0]
    if _index > 0:
        _tindex = _index
        while _tindex < len(df):
            _tindex += 1
            if df['isOpen'][_tindex] == 1:
                return df['calendarDate'][_tindex]
    raise Exception("can not find post trading day.")

def get_highest_time(code_id, start_date, pre_close_price):
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    post_date = get_post_trading_day(start_date)
    date_info = ts.get_hist_data(code=code_id, start=start_date, end=post_date, ktype='5')
    if date_info is not None:
        date_info = date_info.reset_index(drop=False)
        tmp_df = date_info[['close','date']].sort_values(by = 'date', ascending = True)
        for index, cur_price in tmp_df['close'].iteritems():
            total_p_change = (cur_price - pre_close_price) * 100 / pre_close_price
            if total_p_change > 9.8:
                return tmp_df['date'][index]
    return -1

def get_real_time_info():
    all_info = None
    stock_infos = get_classified_stocks()
    stock_nums = len(stock_infos)
    i = 0
    start_index = 0
    while start_index < stock_nums:
        if start_index + 800 > stock_nums:
            end_index = stock_nums - 1
        else:
            end_index = start_index + 800 -1
        stock_codes = stock_infos['code'][start_index:end_index]
        _info = ts.get_realtime_quotes(stock_codes)
        if start_index == 0:
            all_info = _info
        else:
            frames = [all_info, _info]
            all_info = pd.concat(frames, ignore_index = True)
        start_index = end_index + 1
    outstandings = []
    for index, code_id in all_info['code'].iteritems():
        outstanding = stock_infos.query('code=="%s"' % code_id).outstanding.values[0] * 1000000
        outstandings.append(outstanding)
    all_info['limit-up-time'] = 0
    all_info['limit-dowm-time'] = 0
    all_info['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    all_info['outstanding'] = outstandings
    all_info = all_info[(all_info['volume'].astype(float) > 0) & (all_info['outstanding'] > 0)]
    all_info['turnover'] = all_info['volume'].astype(float).divide(all_info['outstanding'])
    all_info['p_change'] = 100 * (all_info['price'].astype(float) - all_info['pre_close'].astype(float)).divide(all_info['pre_close'].astype(float))
    return all_info

def daily_analysis(data_times):
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    stock_infos = get(engine, SQL % 'info')

def collect_concept_volume_price(data_times):
    table = 'concept'
    engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
    concepts = get(engine, SQL % table)
    stock_infos = get(engine, SQL % 'info')
    pydate_array = data_times.to_pydatetime()
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    tables = get_all_tables(DB_USER,DB_PASSWD,DB_NAME,DB_HOSTNAME)
    infos = dict()
    for _date in date_only_array:
        if is_trading_day(_date):
            for index,row in concepts.iterrows():
                concept_name = row['c_name']
                infos[concept_name] = list()
                codes = json.loads(row['code'])
                for code_id in codes:
                    if is_code_exists(code_id):
                        if is_after_release(code_id, _date):
                            if code_id in tables:
                                hist_data = get_hist_data(engine, code_id, _date)
                                rate = hist_data['p_change']
                                if len(rate) > 0 and rate[0] > 5:
                                    volume = hist_data['volume'][0]
                                    pre_price = hist_data['close'][0] + hist_data['price_change'][0]
                                    c_index = stock_infos[stock_infos.code == code_id].index.tolist()[0]
                                    code_name = stock_infos.loc[c_index, 'name']
                                    up_date = get_highest_time(code_id, _date, pre_price)
                                    infos[concept_name].append((code_id, code_name, rate[0], up_date, volume))
    return infos

def get_average_price(market = MARKET_ALL, start_date='2017-01-01', end_date = None):
    _date = today() if end_date is None else end_date
    stock_infos = get_classified_stocks(market)
    total_data = None
    for _index, code_id in stock_infos['code'].iteritems():
        try:
            tmp_data = ts.get_h_data(code_id,start=start_date,end=_date,retry_count = 10)
            if tmp_data is not None:
                tmp_data['code'] = code_id
                tmp_data['outstanding'] = stock_infos.query('code=="%s"' % code_id).outstanding.values[0] * 10000000
                tmp_data = tmp_data.reset_index(drop=False)
                total_data = tmp_data if _index == 0 else pd.concat([total_data,tmp_data],ignore_index=True).drop_duplicates()
        except urllib2.URLError:
            print "get %s failed" % code_id
            pass
    _data = total_data[total_data.date == _date]
    _tmp_data = _data[['code', 'close', 'outstanding']]
    num = 0
    total_volume = 0
    total_price = 0
    total_val_price = 0
    for index, _row in _tmp_data.iterrows():
        print _row['code']
        _price = _row['close']
        _volume = _row['outstanding']
        num += 1
        total_val_price += _price
        total_volume += _volume
        total_price += _price * _volume
    return total_price/total_volume, total_val_price/num

##############################################################
#average price of stock
##############################################################
#weighted_avaerage_price,avaerage_price = get_average_price()
#print weighted_avaerage_price,avaerage_price
##############################################################

##############################################################
##collect dayily concept info
##############################################################
#data_times = pd.date_range('8/1/2017', periods=1, freq='D')
#concept_infos = collect_concept_volume_price(data_times)
#for c_name, c_info in concept_infos.items():
#    if len(c_info) > 3:
#        print "=================================================================S"
#        print c_name
#        print json.dumps(c_info, encoding="UTF-8", ensure_ascii=False, indent = 4)
#        print "=================================================================E"
##############################################################

#===========================================================
#print json.dumps(concept_infos, encoding="UTF-8", ensure_ascii=False, indent = 4)
#stock_infos = get_stock_volumes(MARKET_ALL, data_times)
#print stock_infos.sort_values(by = 'turnover')
#print is_sub_new_stock('20170525')
#print is_trading_day("2017-12-26") 
#print is_trading_day("2017-12-03") 
#print is_after_release('600476', '2017-12-26')
#engine = create_engine('mysql://root:123456@localhost/stock?charset=utf8')
#stock_turnover_rates.to_sql('turnover',engine,if_exists='replace',index=False) 
#==============================================

#############################################################
##daily index analysis
#############################################################
def get_daily_stock_info():
    _date = datetime.now().strftime('%Y-%m-%d')
    if is_trading_day(_date):
        table = 'today'
        static_table = 'today'
        engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8' % (DB_USER,DB_PASSWD,DB_HOSTNAME,DB_NAME))
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (9,24,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (11,31,0)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        aft_open_hour,aft_open_minute,aft_open_second = (13,0,0)
        aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
        aft_close_hour,aft_close_minute,aft_close_second = (15,0,0)
        aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
        now_time = datetime(y,m,d,14,0,0)
        p_changes = []
        p_changes_data = DataFrame(columns=[str(index - 10) for index in range(20)])
        p_changes_data['date'] = now_time
        while (mor_open_time < now_time < mor_close_time) or (aft_open_time < now_time < aft_close_time):
            now_time = datetime.now()
            data = get_real_time_info()
            if data is not None:
                new_data = data[['date', 'name','code','turnover','p_change','price','limit-up-time','limit-down-time']]
                for index, code_id in new_data['code'].iteritems():
                    p_change = new_data.query('code=="%s"' % code_id).p_change.values[0]
                    if p_change > 9.9:
                        new_data['limit-up-time'][index] = now_time
                    if p_change < -9.9:
                        new_data['limit-down-time'][index] = now_time
                    for i in range(20):
                        if i - 10 < p_change <= i - 9:
                            p_changes[i] += 1
            new_data = new_data[new_data['limit-up-time'] != 0 or new_data['limit-down-time'] != 0]
            p_changes.append(now_time)
            p_changes_data.loc[0] = p_changes 
            set(engine,new_data,table)
            set(engine,p_changes_data,static_table)
            time.sleep(60)

get_daily_stock_info()
