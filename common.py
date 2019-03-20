# coding=utf-8
import os
import re
import sys
import json
import time
import copy
import signal
import random
import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
from base.credis import CRedis
from gevent.pool import Pool
from multiprocessing import Process
from datetime import datetime, timedelta
logger = getLogger(__name__)

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

def _fprint(obj):
    print("***************************s")
    print(obj)
    print("***************************e")

def get_years_between(start, end):
    num_of_years = end - start + 1
    year_format = time.strftime("%Y", time.strptime(str(start), "%Y"))
    data_times = pd.date_range(year_format, periods = num_of_years, freq='Y')
    year_only_array = np.vectorize(lambda s: s.strftime('%Y'))(data_times.to_pydatetime())
    return year_only_array.tolist()

def get_dates_array(start_date, end_date, dformat = "%Y-%m-%d", asending = False):
    num_days = delta_days(start_date, end_date, dformat)
    start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(start_date, dformat))
    data_times = pd.date_range(start_date_dmy_format, periods=num_days, freq='D')
    date_only_array = np.vectorize(lambda s: s.strftime(dformat))(data_times.to_pydatetime())
    if asending: return date_only_array
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

def get_day_nday_after(date, num, dformat = "%Y%m%d"):
    t = time.strptime(date, dformat)
    y, m, d = t[0:3]
    _date = datetime(y, m, d) + timedelta(num)
    return _date.strftime(dformat)

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
    return CRedis(host = ct.REDIS_HOST, port = ct.REDIS_PORT, decode_responses = False) 

def df_delta(pos_df, neg_df, subset_list, keep = False):
    pos_df = pos_df.append(neg_df)
    pos_df = pos_df.append(neg_df)
    return pos_df.drop_duplicates(subset=subset_list, keep=False)

def get_market_name(code):
    if code.startswith("6"):
        return ct.SHZB
    elif code.startswith("000") or code.startswith("001"):
        return ct.SZZB
    elif code.startswith("002"):
        return ct.ZXBZ
    elif code.startswith("300"):
        return ct.SCYB
    return None

def get_security_exchange_name(stock_code):
    if (stock_code.startswith("6") or stock_code.startswith("500") or stock_code.startswith("550") or stock_code.startswith("510") or stock_code.startswith("8")):
        return "sh"
    elif (stock_code.startswith("00") or stock_code.startswith("30") or stock_code.startswith("150") or stock_code.startswith("159")):
        return "sz"
    else:
        return "none"

def add_prifix(stock_code):
    if get_security_exchange_name(stock_code) == "sh":
        return "SH." + stock_code
    else:
        return "SZ." + stock_code

def add_suffix(stock_code):
    if get_security_exchange_name(stock_code) == "sh":
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

def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
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
    from matplotlib.font_manager import FontProperties
    fpath = '/Volumes/data/quant/stock/conf/fonts/PingFang.ttc' if location == "OUT" else '/conf/fonts/PingFang.ttc'
    return FontProperties(fname = fpath)

def df_empty(columns, dtypes, index = None):
    assert len(columns) == len(dtypes)
    df = pd.DataFrame(index = index)
    for c,d in zip(columns, dtypes):
        df[c] = pd.Series(dtype = d)
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

def smart_get(func, *args, **kwargs):
    for i in range(3):
        try:
            return func(*args, **kwargs)
        except:
            time.sleep(2 * (i + 1))
    return None

def int_random(n = 16):
    return ''.join(str(random.choice(range(1, 9))) for _ in range(n))

def float_random(n = 16):
    x = int_random(n)
    return '0.%s' % x

def loads_jsonp(_jsonp):
    try:
        return json.loads(re.match(".*?({.*}).*", _jsonp, re.S).group(1))
    except:
        return None

def get_unfinished_workers(redis_client, key):
    return list(set(code.decode() for code in redis_client.smembers(key)))

def process_concurrent_run(mfunc, all_list, process_num = 2, num = 10, max_retry_times = 10):
    def init_unfinished_workers(redis_client, key, todo_list, overwrite = False):
        if not redis_client.exists(key):
            redis_client.sadd(key, *set(todo_list))
        else:
            if overwrite:
                redis_client.delete(key)
                redis_client.sadd(key, *set(todo_list))
        return list(set(code.decode() for code in redis_client.smembers(key)))
    redis_client = create_redis_obj()
    todo_list = init_unfinished_workers(redis_client, ct.UNFINISHED_WORKS, copy.deepcopy(all_list))
    if len(todo_list) == 0: return False
    if len(todo_list) <= num:
        return concurrent_run(mfunc, todo_list, num = process_num)
    else:
        jobs = list()
        last_length = len(todo_list)
        while len(todo_list) > 0:
            for x in range(process_num):
                p = Process(target = thread_concurrent_run, args=(mfunc, redis_client, ct.UNFINISHED_WORKS), kwargs={'num': num, 'name': x, 'process_num': process_num})
                jobs.append(p)
            for j in jobs: j.start()
            for j in jobs: j.join()
            todo_list = get_unfinished_workers(redis_client, ct.UNFINISHED_WORKS)
            if len(todo_list) == last_length:
                logger.info("left todo list length:%s" % todo_list)
                return False
    return True

def thread_concurrent_run(mfunc, redis_client, key, num = 10, name = 0, process_num = 2):
    obj_pool = Pool(num)
    all_list = get_unfinished_workers(redis_client, key)
    av_num = int(len(all_list) / process_num) + process_num
    i_start = name * av_num
    i_end = i_start + av_num
    todo_list = all_list[i_start:i_end]
    if 0 == len(todo_list): sys.exit(True)
    for result in obj_pool.imap_unordered(mfunc, todo_list):
        if True == result[1]: redis_client.srem(key, result[0])
    obj_pool.join(timeout = 10)
    obj_pool.kill()
    sys.exit(True) if len(todo_list) == 0 else sys.exit(False)

def concurrent_run(mfunc, all_list, num = 10, max_retry_times = 10):
    failed_count = 0
    obj_pool = Pool(num)
    todo_list = copy.deepcopy(all_list)
    while len(todo_list) > 0:
        is_failed = False
        for result in obj_pool.imap_unordered(mfunc, todo_list):
            if True == result[1]: 
                todo_list.remove(result[0])
            else:
                is_failed = True
        if is_failed:
            if failed_count > max_retry_times:
                obj_pool.join(timeout = 10)
                obj_pool.kill()
                return False
            failed_count += 1
            time.sleep(3)
    obj_pool.join(timeout = 10)
    obj_pool.kill()
    return True

def is_df_has_unexpected_data(df):
    if not df[df.isin([np.nan, np.inf, -np.inf]).any(1)].empty:
        return True
    if not df[pd.isnull(df).any(1)].empty:
        return True
    return False

def resample(data, period = 'W-Mon'):
    ohlc_dict = {'open':'first', 'high':'max', 'low':'min', 'close': 'last', 'volume': 'sum', 'amount': 'sum'}
    data['date'] =  pd.to_datetime(data['date'])
    data.set_index('date',inplace = True)
    df = data.resample(period, closed = 'left', label = 'left').agg(ohlc_dict).dropna(how='any')
    return df
