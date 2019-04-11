# -*- coding: utf-8 -*-
import time
import calendar
import datetime
from scrapy import Spider
from datetime import datetime, timedelta
class BasicSpider(Spider):
    def get_nday_ago(self, mdate, num, dformat = "%Y.%m.%d"):
        t = time.strptime(mdate, dformat)
        y, m, d = t[0:3]
        _date = datetime(y, m, d) - timedelta(num)
        return _date.strftime(dformat) 

    def get_next_date(self, sdate = datetime.now().strftime('%Y.%m.%d'), target_day = calendar.FRIDAY):
        #func: get next date
        #sdate: str, example: '2017-01-01'
        #tdate: str, example: '2017-01-06'
        tdate = ''
        oneday = timedelta(days = 1)
        sdate = datetime.strptime(sdate, '%Y.%m.%d')
        if sdate.weekday() == target_day: sdate += oneday
        while sdate.weekday() != target_day: 
            sdate += oneday
        tdate = sdate.strftime("%Y.%m.%d")
        return tdate

    def get_tomorrow_date(self, sdate):
        #func: get next date
        #sdate: str, example: '2017.01.01'
        #tdate: str, example: '2017.01.06'
        tdate = ''
        oneday = timedelta(days = 1)
        sdate = datetime.strptime(sdate, '%Y.%m.%d')
        sdate += oneday
        tdate = sdate.strftime("%Y.%m.%d")
        return tdate
