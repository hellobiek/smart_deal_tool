# -*- coding: utf-8 -*-
import time
import numpy as np
import pandas as pd
from datetime import datetime
def quarter(mdate:int):
    """
    获取指定日期属于第几季度
    :param int mdate: yyyymmdd 格式的日期字符串
    :return: 0表示第一季度，1表示第二季度 ...
    """
    year = int(mdate/10000)
    month = int(mdate%10000/100)
    if month in [1, 2, 3]:
        return year, 0
    if month in [4, 5, 6]:
        return year, 1
    if month in [7, 8, 9]:
        return year, 2
    if month in [10, 11, 12]:
        return year, 3
    return None, None

def transfer_date_string_to_int(cdate):
    cdates = cdate.split('-')
    return int(cdates[0]) * 10000 + int(cdates[1]) * 100 + int(cdates[2])

def transfer_int_to_date_string(cdate):
    return time.strftime('%Y-%m-%d', time.strptime(str(cdate), "%Y%m%d"))

def str_to_datetime(mdate:str, dformat = "%Y%m%d"):
    """将字符串转换成datetime类型"""
    return datetime.strptime(mdate, dformat)

def int_to_datetime(mdate:int, dformat = "%Y%m%d"):
    return str_to_datetime(str(mdate), dformat)

def datetime_to_str(mdate, dformat = "%Y%m%d"):
    """将datetime类型转换为日期型字符串,格式为2008-08-02"""
    return mdate.strftime(dformat) 

def datetime_to_int(mdate, dformat = "%Y%m%d"):
    """将datetime类型转换为日期型字符串,格式为2008-08-02"""
    return int(datetime_to_str(mdate, dformat))

def report_date_with(mdate:int):
    """
    根据当前日期获取标准的财报日期
    :param mdate: 指定日期时间yyyymmdd
    :return:
    """
    (myear, mquarter) = quarter(mdate)
    month_day = "%02d%02d" % (int(mdate%10000/100), int(mdate%100))
    if myear > 2001:
        quarterdate = ["1231", "0331", "0630", "0930"]
        if month_day in quarterdate: return mdate
        if mquarter == 0: return int("%d%s" % (int(myear)-1, quarterdate[mquarter]))
        return int("%d%s" % (myear, quarterdate[mquarter]))
    else:
        quarterdate = ["1231", "0630"]
        if month_day in quarterdate: return mdate
        return int("%d%s" % (myear, quarterdate[1])) if mquarter > 1 else int("%d%s" % (int(myear)-1, quarterdate[0]))

def one_report_date_list(mdate):
    date_list = list()
    today = datetime.now()
    target = str_to_datetime(mdate, dformat = "%Y-%m-%d")
    if today.year >= target.year:
        q1 = str_to_datetime("%d0331" % target.year)
        q2 = str_to_datetime("%d0630" % target.year)
        q3 = str_to_datetime("%d0930" % target.year)
        if q3 <= target: date_list.append("%d0930" % target.year)
        elif q2 <= target: date_list.append("%d0630" % target.year)
        elif q1 <= target: date_list.append("%d0331" % target.year)
        else: date_list.append("%d1231" % (target.year - 1))
        if today.year == target.year and today.month == 4:
            date_list.append("%d1231" % (target.year-1))
    return date_list

def report_date_list_with(mdate = None):
    """获取指定日期所有的财报"""
    mtoday = datetime.today() if mdate is None else int_to_datetime(mdate)
    date_list = ["19980630", "19981231", "19990630", "19991231", "20000630", "20001231", "20010630", "20010930", "20011231"]

    if mtoday.year < 2002: return date_list

    idx = 2002
    while idx < mtoday.year:
        date_list.append("%d0331" % idx)
        date_list.append("%d0630" % idx)
        date_list.append("%d0930" % idx)
        date_list.append("%d1231" % idx)
        idx += 1

    # 超过3月份的就需要更新一季度
    target = str_to_datetime("%d0331" % mtoday.year)
    if target <= mtoday:
        date_list.append("%d0331" % mtoday.year)

    target = str_to_datetime("%d0630" % mtoday.year)
    if target <= mtoday:
        date_list.append("%d0630" % mtoday.year)

    target = str_to_datetime("%d0930" % mtoday.year)
    if target <= mtoday:
        date_list.append("%d0930" % mtoday.year)

    target = str_to_datetime("%d1231" % mtoday.year)
    if target <= mtoday:
        date_list.append("%d1231" % mtoday.year)
    return date_list

def prev_report_date_with(mdate):
    """获取指定财报日期的前一个财报日期"""
    date_list = report_date_list_with(mdate)
    # 不存在或者已经是第一个了没有前一份财报了
    smdate = str(mdate)
    if smdate not in date_list: return None
    idx = date_list.index(smdate)
    return int(date_list[idx-1])

def get_years_between(start, end):
    num_of_years = end - start + 1
    year_format = time.strftime("%Y", time.strptime(str(start), "%Y"))
    data_times = pd.date_range(year_format, periods = num_of_years, freq='Y')
    year_only_array = np.vectorize(lambda s: s.strftime('%Y'))(data_times.to_pydatetime())
    return year_only_array.tolist()

def years_ago(years, from_date=None):
    """获取几年前的日期"""
    if from_date is None: from_date = datetime.now()
    try:
        return from_date.replace(year=from_date.year - years)
    except ValueError:
        # Must be 2/29!
        assert from_date.month == 2 and from_date.day == 29 # can be removed
        return from_date.replace(month=2, day=28, year=from_date.year-years)
