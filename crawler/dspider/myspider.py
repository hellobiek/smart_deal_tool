# -*- coding: utf-8 -*-
import time
import calendar
import datetime
from scrapy import Spider
from base.clog import getLogger 
from base.wechat import SendWechat
from common import create_redis_obj
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from twisted.internet.error import TimeoutError
from twisted.internet.error import DNSLookupError
from scrapy.spidermiddlewares.httperror import HttpError
class BasicSpider(Spider):
    name = ''
    redis = create_redis_obj()
    logger = getLogger(__name__)
    message_client = SendWechat()
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

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return self.redis.sismember(table_name, cdate)
        return False

    def errback_httpbin(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            msg = 'HttpError on {}'.format(response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            msg = 'DNSLookupError on {}'.format(request.url)
        elif failure.check(TimeoutError):
            request = failure.request
            msg = 'TimeoutError on {}'.format(request.url)
        else:
            request = failure.request
            msg = 'UnknownError on {}'.format(request.url)
        self.logger.error(msg)
        self.message_client.send_message(self.name, msg)

    def value_of_none(self, value, default_val = 0):
        return default_val if value is None else value
