# coding=utf-8
import os
import sys
import json
import time
import signal
import redis
import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from crack_bmp import crack_bmp
from datetime import datetime,timedelta

def trace_func(*dargs, **dkargs):
    def wrapper(func):
        def _wrapper(*args, **kargs):
            if 'log' not in dkargs:
                print('Start %s(%s, %s)...' % (func.__name__, args, kargs))
            else:
                dkargs['log'].debug('Start %s(%s, %s)...' % (func.__name__, args, kargs))
            return func(*args, **kargs)
        return _wrapper
    return wrapper

def gint(x):
    if x > 9.95:
        return 10
    elif 9 <= x <= 9.95:
        return 9 
    elif x < -9.95:
        return -10
    elif -9.95 <= x <= -9:
        return -9
    else:
        return int(x)

def get_verified_code(tmp_buff):
    with open('/tmp/1.jpg','wb') as verify_pic:
        verify_pic.write(tmp_buff)
    return crack_bmp().decode_from_file('/tmp/1.jpg')

def _fprint(obj):
    print("***************************s")
    print(obj)
    print("***************************e")

def get_years_between(start, end):
    num_of_years = end - start + 1
    year_format = time.strftime("%Y", time.strptime(str(start), "%Y"))
    data_times = pd.date_range(year_format, periods = num_of_years, freq='Y')
    year_only_array = np.vectorize(lambda s: s.strftime('%Y'))(data_times.to_pydatetime())
    return year_only_array

def get_dates_array(start_date, end_date, dformat = "%Y-%m-%d"):
    num_days = delta_days(start_date, end_date, dformat)
    start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, dformat))
    data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
    date_only_array = np.vectorize(lambda s: s.strftime(dformat))(data_times.to_pydatetime())
    date_only_array = date_only_array[::-1]
    return date_only_array

def delta_days(_from, _to, dformat = "%Y-%m-%d"):
    _from = time.strptime(_from, dformat)
    _to = time.strptime(_to, dformat)
    _from = datetime(_from[0],_from[1],_from[2])
    _to = datetime(_to[0],_to[1],_to[2])
    return (_to - _from).days + 1

def number_of_days(pre_pos, pos):
    return pos - pre_pos

def is_afternoon(now_time = None):
    if now_time is None:now_time = datetime.now()
    _date = now_time.strftime('%Y-%m-%d')
    y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
    mor_open_hour,mor_open_minute,mor_open_second = (12,0,0)
    mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
    mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
    mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
    return (mor_open_time < now_time < mor_close_time)

def get_day_nday_ago(date, num, dformat = "%Y%m%d"):
    t = time.strptime(date, dformat)
    y, m, d = t[0:3]
    _date = datetime(y, m, d) - timedelta(num)
    return _date.strftime(dformat)

def is_trading_time(now_time = None):
    if now_time is None:now_time = datetime.now()
    _date = now_time.strftime('%Y-%m-%d')
    y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
    mor_open_hour,mor_open_minute,mor_open_second = (9,13,0)
    mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
    mor_close_hour,mor_close_minute,mor_close_second = (11,32,0)
    mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
    aft_open_hour,aft_open_minute,aft_open_second = (12,58,0)
    aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
    aft_close_hour,aft_close_minute,aft_close_second = (15,2,0)
    aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
    return (mor_open_time < now_time < mor_close_time) or (aft_open_time < now_time < aft_close_time)

def create_redis_obj(host = ct.REDIS_HOST, port = ct.REDIS_PORT, decode_responses = False):
    pool = redis.ConnectionPool(host = host, port = port, decode_responses = decode_responses)
    return redis.StrictRedis(connection_pool = pool)

def df_delta(pos_df, neg_df, subset_list, keep = False):
    pos_df = pos_df.append(neg_df)
    pos_df = pos_df.append(neg_df)
    return pos_df.drop_duplicates(subset=subset_list, keep=False)

def get_market_name(stock_code):
    if (stock_code.startswith("6") or stock_code.startswith("500") or stock_code.startswith("550") or stock_code.startswith("510") or stock_code.startswith("8")):
        return "sh"
    elif (stock_code.startswith("00") or stock_code.startswith("30") or stock_code.startswith("150") or stock_code.startswith("159")):
        return "sz"
    else:
        return "none"

def add_prifix(stock_code):
    if get_market_name(stock_code) == "sh":
        return "SH." + stock_code
    else:
        return "SZ." + stock_code

def add_suffix(stock_code):
    if get_market_name(stock_code) == "sh":
        return stock_code + ".SH"
    else:
        return stock_code + ".SZ"

def get_available_tdx_server(api):
    for k,v in ct.TDX_SERVERS.items():
        ip, port = ct.TDX_SERVERS[k][1].split(":")
        if api.connect(ip, int(port)): return ip, int(port)
    raise Exception("no server can be connected")

def get_market(code):
    if (code.startswith("6") or code.startswith("500") or code.startswith("550") or code.startswith("510")) or code.startswith("7"):
        return ct.MARKET_SH
    elif (code.startswith("00") or code.startswith("30") or code.startswith("150") or code.startswith("159")):
        return ct.MARKET_SZ
    else:
        return ct.MARKET_OTHER

epoch = datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

def add_index_prefix(code):
    prestr = "SH" if code.startswith('0') else "SZ"
    return "%s.%s" % (prestr, code)

def get_index_list():
    alist = list()
    for key in ct.INDEX_DICT.keys():
        key_str = add_index_prefix(key)
        alist.append(key_str)
    return alist

def get_chinese_font(location = "IN"):
    fpath = '/Volumes/data/quant/stock/conf/fonts/PingFang.ttc' if location == "OUT" else '/conf/fonts/PingFang.ttc'
    return FontProperties(fname = fpath)

def df_empty(columns, dtypes, index = None):
    assert len(columns)==len(dtypes)
    df = pd.DataFrame(index=index)
    for c,d in zip(columns, dtypes):
        df[c] = pd.Series(dtype=d)
    return df

def get_real_trading_stocks(fpath = ct.USER_FILE):
    with open(fpath) as f:
        infos = json.load(f)
    return infos

def get_tushare_client(fpath = ct.TUSHAE_FILE):
    with open(fpath) as f: key_info = json.load(f)
    return ts.pro_api(key_info['key'])

def transfer_date_string_to_int(cdate):
    cdates = cdate.split('-')
    return int(cdates[0]) * 10000 + int(cdates[1]) * 100 + int(cdates[2])

def transfer_int_to_date_string(cdate):
    return time.strftime('%Y-%m-%d', time.strptime(str(cdate), "%Y%m%d"))

def kill_process(pstring):
    for line in os.popen("ps ax | grep " + pstring + " | grep -v grep"):
        fields = line.split()
        pid = fields[0]
        os.kill(int(pid), signal.SIGKILL)
