# coding=utf-8
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
import os
import wget
import requests
import ctypes
import struct
import zipfile
import const as ct
import numpy as np
import pandas as pd
import time
import datetime
from datetime import datetime, timedelta
from log import getLogger
from common import get_security_exchange_name, get_day_nday_ago, get_dates_array
from models import TickTradeDetail, TickDetailModel
logger = getLogger(__name__)
pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def unsigned2signed(value):
    return ctypes.c_int32(value).value

def signed2unsigned(value, b = 32):
    if 32 == b:
        return ctypes.c_uint32(value).value
    elif 8 == b:
        return ctypes.c_uint8(value).value
    elif 16 == b:
        return ctypes.c_uint16(value).value

def int_overflow(val):
    maxint = 2147483647
    if not -maxint - 1 <= val <= maxint:
        val = (val + (maxint + 1)) % (2 * (maxint + 1)) - maxint - 1
    return val

def unsigned_left_shitf(n,i):
    if n < 0: n = ctypes.c_uint32(n).value
    if i < 0: return int_overflow(n >> abs(i))
    return int_overflow(n << i)

def read4(tic_detail_bytes):
    left = tic_detail_bytes[0:4]
    tic_detail_bytes = tic_detail_bytes[4:]
    return left, tic_detail_bytes

def read1(tic_detail_bytes):
    left = tic_detail_bytes[0:1]
    tic_detail_bytes = tic_detail_bytes[1:]
    return signed2unsigned(struct.unpack('b', left)[0], 8), tic_detail_bytes

def read2(tic_detail_bytes):
    left = tic_detail_bytes[0:2]
    tic_detail_bytes = tic_detail_bytes[2:]
    return signed2unsigned(struct.unpack('H', left)[0], 16), tic_detail_bytes

def dict2list(cdic:dict):
    keys = cdic.keys()
    vals = cdic.values()
    return [(key, val) for key, val in zip(keys, vals)]

def get_price_list(alist):
    pos_list = [x for x in alist if int(x[1], 16) > 0]
    pos_list = sorted(pos_list, key=lambda x: int(x[1], 16), reverse=False)
    neg_list = [x for x in alist if int(x[1], 16) < 0]
    neg_list = sorted(neg_list, key=lambda x: int(x[1], 16), reverse=False)
    pos_list.extend(neg_list)
    return pos_list

def parse_tick_price(ttd_list, tic_detail_bytes, tdm):
    tmp_size = 32
    time_list = sorted(dict2list(ct.HASH_TABLE_DATETIME), key=lambda x: int(x[1], 16), reverse=False)
    price_list = get_price_list(dict2list(ct.HASH_TABLE_PRICE))
    left, tic_detail_bytes = read4(tic_detail_bytes)
    tick_data_item = struct.unpack('I', left)[0]
    for _ in range(1, tdm.count):
        #解析类型
        _type = unsigned_left_shitf(tick_data_item, -31)
        _type = "买入" if 0 == _type else "卖出"
		#解析时间
        tick_data_item = unsigned_left_shitf(tick_data_item, 1)
        tmp_size -= 1
        if 0 == tmp_size:
            left, tic_detail_bytes = read4(tic_detail_bytes)
            tick_data_item = struct.unpack('I', left)[0]
            tmp_size = 32
        tmp_check_sum = 3
        tmp_index, tick_data_item, tic_detail_bytes, tmp_size = time_recursion(tmp_check_sum, tick_data_item, tmp_size, tic_detail_bytes, time_list)
        _time = ttd_list[len(ttd_list) - 1].dtime + tmp_index
		#解析价格
        tmp_check_sum = 3
        tmp_index, tick_data_item, tic_detail_bytes, tmp_size = price_recursion(tmp_check_sum, tick_data_item, tmp_size, tic_detail_bytes, price_list)
        if tmp_index != len(ct.HASH_TABLE_PRICE) - 1:
            _price = ttd_list[len(ttd_list)-1].price + tmp_index
        else:
            tmp_check_sum = 0
            tmp_index = 32
            while tmp_index > 0:
                tmp_index -= 1
                tmp_check_sum = unsigned_left_shitf(tmp_check_sum, 1) | unsigned_left_shitf(tick_data_item, -31)
                tick_data_item = unsigned_left_shitf(tick_data_item, 1)
                tmp_size -= 1
                if 0 == tmp_size:
                    left, tic_detail_bytes = read4(tic_detail_bytes)
                    tick_data_item = struct.unpack('I', left)[0]
                    tmp_size = 32
            _price = ttd_list[len(ttd_list)-1].price + tmp_index
        ttd_list.append(TickTradeDetail(_time, _price, 0, _type))
    return ttd_list

def time_recursion(tmp_check_sum, tick_data_item, tmp_size, tic_detail_bytes, klist):
    tmp_check_sum = unsigned_left_shitf(tmp_check_sum, 1) | unsigned_left_shitf(tick_data_item, -31)
    tick_data_item = unsigned_left_shitf(tick_data_item, 1)
    tmp_size -= 1
    if 0 == tmp_size:
        left, tic_detail_bytes = read4(tic_detail_bytes)
        tick_data_item = struct.unpack('I', left)[0]
        tmp_size = 32
    tmp_index = 0
    while int(klist[tmp_index][1], 16) != tmp_check_sum:
        if int(klist[tmp_index][1], 16) < tmp_check_sum:
            tmp_index += 1
            if tmp_index < len(klist): continue
        tmp_check_sum = unsigned_left_shitf(tmp_check_sum, 1) | unsigned_left_shitf(tick_data_item, -31)
        tick_data_item = unsigned_left_shitf(tick_data_item, 1)
        tmp_size -= 1
        if 0 == tmp_size:
            left, tic_detail_bytes = read4(tic_detail_bytes)
            tick_data_item = struct.unpack('I', left)[0]
            tmp_size = 32
        tmp_index = 0
    return klist[tmp_index][0], tick_data_item, tic_detail_bytes, tmp_size

def price_recursion(tmp_check_sum, tick_data_item, tmp_size, tic_detail_bytes, klist):
    tmp_check_sum = unsigned_left_shitf(tmp_check_sum, 1) | unsigned_left_shitf(tick_data_item, -31)
    tick_data_item = unsigned_left_shitf(tick_data_item, 1)
    tmp_size -= 1
    if 0 == tmp_size:
        left, tic_detail_bytes = read4(tic_detail_bytes)
        tick_data_item = struct.unpack('I', left)[0]
        tmp_size = 32
    tmp_index = 0
    while int(klist[tmp_index][1], 16) != tmp_check_sum:
        if signed2unsigned(tmp_check_sum) > 0x3FFFFFF or int(klist[tmp_index][1], 16) <= tmp_check_sum:
            tmp_index += 1
            if tmp_index < len(klist): continue
        tmp_check_sum = unsigned_left_shitf(tmp_check_sum, 1) | unsigned_left_shitf(tick_data_item, -31)
        tick_data_item = unsigned_left_shitf(tick_data_item, 1)
        tmp_size -= 1
        if 0 == tmp_size:
            left, tic_detail_bytes = read4(tic_detail_bytes)
            tick_data_item = struct.unpack('I', left)[0]
            tmp_size = 32
        tmp_index = 0
    return klist[tmp_index][0], tick_data_item, tic_detail_bytes, tmp_size
                    
def parse_tick_detail(td_bytes, tdm):
    ttd_list = list()
    _type = "买入" if 0 == unsigned_left_shitf(tdm.type, -15) else "卖出"
    ttd = TickTradeDetail(tdm.dtime, tdm.price, tdm.volume, _type)
    ttd_list.append(ttd)
    #解析交易时间及价格信息
    ttd_list = parse_tick_price(ttd_list, td_bytes, tdm)
    #解析成交量
    volume_buffer = td_bytes[tdm.vol_offset : (tdm.vol_offset + tdm.vol_size)]
    for i in range(1, tdm.count):
        result_vol = 0
        byte_volume, volume_buffer = read1(volume_buffer)
        if byte_volume <= 252:
            result_vol = int(byte_volume)
        elif byte_volume == 253:
            tmp_vol, volume_buffer = read1(volume_buffer)
            result_vol = int(tmp_vol) + int(byte_volume)
            result_vol = signed2unsigned(result_vol, 16)
        elif byte_volume == 254:
            tmp_vol, volume_buffer = read2(volume_buffer)
            result_vol = int(tmp_vol) + int(byte_volume)
        else:
            tmp_vol1, volume_buffer = read1(volume_buffer)
            tmp_vol2, volume_buffer = read2(volume_buffer)
            result_vol = int(0xFFFF * int(tmp_vol1) + int(tmp_vol2) + 0xFF)
        ttd_list[i].volume = result_vol
        ttd_list[i].dtime = set_trade_time(ttd_list[i].dtime)
        ttd_list[i].price = ttd_list[i].price / 100
    ttd_list[0].dtime = set_trade_time(-5)
    ttd_list[0].price = ttd_list[0].price / 100
    return ttd_list

def set_trade_time(time_val):
    result = time_val + 570 if time_val <= 120 else time_val + 660
    _hour = (result / 60) % 24
    _minute = result % 60
    return "%02d:%02d" % (_hour, _minute)

def parse_tick_item(data, code):
    tick_item_bytes = data[:20]
    tic_detail_bytes = data[20:]
    (sdate, scount, svol_offset, svol_size, stype, sprice, svolume) = struct.unpack("iHHHHii", tick_item_bytes)
    stime = ctypes.c_uint8(stype).value
    tdm = TickDetailModel(sdate, stime, sprice, svolume, scount, stype, svol_offset, svol_size)
    return parse_tick_detail(tic_detail_bytes, tdm)

def read_tick(filename, code_id):
    if not os.path.exists(filename): return pd.DataFrame()
    with open(filename, 'rb') as fobj:
        market_id = ct.MARKET_SH if 'sh' == get_security_exchange_name(code_id) else ct.MARKET_SZ
        stockCount = struct.unpack('<h', fobj.read(2))[0]
        for idx in range(stockCount):
            (market, code, _, date, t_size, pre_close) = struct.unpack("B6s1siif", fobj.read(20))
            code = code.decode()
            raw_tick_data = fobj.read(t_size)
            if code == code_id and market == market_id:
                if t_size == 20: return pd.DataFrame()
                ttd_list = parse_tick_item(raw_tick_data, code)
                if len(ttd_list) == 0: return pd.DataFrame()
                dict_list = list()
                for ttd in ttd_list:
                    cdict = {'time': ttd.dtime, 'price': ttd.price, 'volume': ttd.volume, 'type': ttd.type}
                    dict_list.append(cdict)
                df = pd.DataFrame(dict_list)
                df = adjust_time(df)
                df['change'] = df['price'] - df["price"].shift(1)
                df.at[0, 'change'] = df.loc[0]['price'] - pre_close
                df['amount'] = df['price'] * df['volume']
                df = df[['time','price','change','volume', 'amount', 'type']]
                return df.round(2)
    return pd.DataFrame()

def adjust_time(df):
    s_index = 0
    e_index = 0
    time_list = df['time']
    time_list_length = len(time_list)
    for _index in range(1, time_list_length):
        if time_list[s_index] == time_list[_index] and _index < time_list_length - 1:
            e_index = _index
        else:
            if time_list[s_index] == time_list[_index] and _index == time_list_length - 1: e_index = _index
            _length = (e_index - s_index + 1)
            time_delta = int(60/_length)
            for _tmp_index in range(_length):
                z_index = s_index + _tmp_index
                df.at[z_index, 'time'] = "%s:%02d" % (df.loc[z_index]['time'], time_delta * _tmp_index)
            s_index = e_index + 1
            e_index = s_index
            if _index == time_list_length - 1 and s_index == _index:
                #special deal with date where last row is 14:59 but notleast row is 14:55
                df.at[s_index, 'time'] = "%s:%02d" % (df.loc[s_index]['time'], 0)
    return df

def exists(path):
    r = requests.head(path)
    return r.status_code == requests.codes.ok

def download(output_directory):
    _date = get_day_nday_ago(datetime.now().strftime('%Y%m%d'), num = 50)
    start_date_dmy_format = time.strftime("%m/%d/%Y", time.strptime(_date, "%Y%m%d"))
    data_times = pd.date_range(start_date_dmy_format, periods=10, freq='D')
    date_only_array = np.vectorize(lambda s: s.strftime('%Y%m%d'))(data_times.to_pydatetime())
    date_only_array = date_only_array[::-1]
    for _date in date_only_array:
        filename = "%s.zip" % _date
        url = "http://www.tdx.com.cn/products/data/data/2ktic/%s" % filename
        filepath = "%s/%s" % (output_directory, filename)
        try:
            if os.path.exists(filepath):
                logger.debug("%s existed" % filepath)
                continue
            if not exists(url): 
                logger.debug("%s not exists" % filename)
                continue
            wget.download(url, out=output_directory)
        except Exception as e:
            logger.error(e)

def unzip(file_path, tic_dir):
    zip_file = zipfile.ZipFile(file_path)
    for names in zip_file.namelist():
        tic_file = os.path.join(tic_dir, names)
        if not os.path.exists(tic_file):
            zip_file.extract(names, tic_dir)
    zip_file.close()

if __name__ == "__main__":
    code_id = '880001'
    tickname = '20180822.tic'
    ticname = os.path.join('/Volumes/data/quant/stock/data/tdx/tic', tickname)
    df = read_tick(ticname, code_id)
    print(df)
