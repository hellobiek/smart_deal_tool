# -*- coding: utf-8 -*-
import time
import calendar
import datetime
from scrapy import Spider
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from twisted.internet.error import TimeoutError
from twisted.internet.error import DNSLookupError
from scrapy.spidermiddlewares.httperror import HttpError
class BasicSpider(Spider):
    def get_nday_ago(self, mdate, num, dformat = "%Y.%m.%d"):
        t = time.strptime(mdate, dformat)
        y, m, d = t[0:3]
        _date = datetime(y, m, d) - timedelta(num)
        return _date.strftime(dformat) 

    def get_next_date(self, sdate, target_day = calendar.FRIDAY, dformat = '%Y.%m.%d'):
        #func: get next date
        #sdate: str, example: '2017-01-01'
        #tdate: str, example: '2017-01-06'
        oneday = timedelta(days = 1)
        sdate = datetime.strptime(sdate, dformat)
        if sdate.weekday() == target_day: sdate += oneday
        while sdate.weekday() != target_day: 
            sdate += oneday
        tdate = sdate.strftime(dformat)
        return tdate

    def get_next_month(self, smonth):
        onemonth = relativedelta(months=1)
        smonth = datetime.strptime(smonth, '%Y年%m月')
        smonth += onemonth
        tmonth = smonth.strftime("%Y年%m月")
        return tmonth

    def get_nmonth_ago(self, smonth, num, dformat = "%Y年%m月"):
        deltamonth = relativedelta(months=num)
        smonth = datetime.strptime(smonth, dformat)
        smonth -= deltamonth
        tmonth = smonth.strftime(dformat)
        return tmonth

    def get_tomorrow_date(self, sdate, dformat = '%Y.%m.%d'):
        #func: get next date
        #sdate: str, example: '2017.01.01'
        #tdate: str, example: '2017.01.06'
        oneday = timedelta(days = 1)
        sdate = datetime.strptime(sdate, dformat)
        sdate += oneday
        while sdate.weekday() > 4: sdate += oneday 
        tdate = sdate.strftime(dformat)
        return tdate

    def errback_httpbin(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            print('HttpError on %s' % response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            print('DNSLookupError on %s' %request.url)
        elif failure.check(TimeoutError):
            request = failure.request
            print('TimeoutError on %s' % request.url)
        else:
            request = failure.request
            print('UnknownError on %s' % request.url)
