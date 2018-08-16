# coding=utf-8
import sys
import time
import redis
from tempfile import TemporaryFile
import datetime
from datetime import datetime,timedelta
import const as ct
from crack_bmp import crack_bmp

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

def delta_days(_from, _to):
    _from = time.strptime(_from,"%Y-%m-%d")
    _to = time.strptime(_to,"%Y-%m-%d")
    _from = datetime(_from[0],_from[1],_from[2])
    _to = datetime(_to[0],_to[1],_to[2])
    return (_to - _from).days

def is_afternoon(now_time = None):
    if now_time is None:now_time = datetime.now()
    _date = now_time.strftime('%Y-%m-%d')
    y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
    mor_open_hour,mor_open_minute,mor_open_second = (12,0,0)
    mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
    mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
    mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
    return (mor_open_time < now_time < mor_close_time)

def is_trading_time(now_time = None):
    if now_time is None:now_time = datetime.now()
    _date = now_time.strftime('%Y-%m-%d')
    y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
    mor_open_hour,mor_open_minute,mor_open_second = (9,10,0)
    mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
    mor_close_hour,mor_close_minute,mor_close_second = (11,35,0)
    mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
    aft_open_hour,aft_open_minute,aft_open_second = (12,55,0)
    aft_open_time = datetime(y,m,d,aft_open_hour,aft_open_minute,aft_open_second)
    aft_close_hour,aft_close_minute,aft_close_second = (15,5,0)
    aft_close_time = datetime(y,m,d,aft_close_hour,aft_close_minute,aft_close_second)
    return (mor_open_time < now_time < mor_close_time) or (aft_open_time < now_time < aft_close_time)

def delta_days(_from, _to):
    _from = time.strptime(_from,"%Y-%m-%d")
    _to = time.strptime(_to,"%Y-%m-%d")
    _from = datetime(_from[0],_from[1],_from[2])
    _to = datetime(_to[0],_to[1],_to[2])
    return (_to - _from).days

def create_redis_obj(host = ct.REDIS_HOST, port = ct.REDIS_PORT, decode_responses = False):
    pool = redis.ConnectionPool(host = host, port = port, decode_responses = decode_responses)
    return redis.StrictRedis(connection_pool=pool)

def df_delta(pos_df, neg_df, subset_list, keep = False):
    pos_df = pos_df.append(neg_df)
    pos_df = pos_df.append(neg_df)
    return pos_df.drop_duplicates(subset=subset_list, keep=False)

def get_market_name(stock_code):
    if (stock_code.startswith("6") or stock_code.startswith("500") or stock_code.startswith("550") or stock_code.startswith("510")):
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
