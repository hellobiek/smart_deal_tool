# -*- coding: utf-8 -*-
import time
import calendar
import datetime
import const as ct
import pandas as pd
from pathlib import Path
from scrapy import Spider
from common import create_redis_obj
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from twisted.internet.error import TimeoutError
from twisted.internet.error import DNSLookupError
from scrapy.spidermiddlewares.httperror import HttpError
class BasicSpider(Spider):
    name = ''
    status = False
    message = ""
    redis = create_redis_obj()
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
        self.status = False
        self.message = msg

    def value_of_none(self, value, default_val = 0):
        return default_val if value is None else value

    def collect_spider_info(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df = pd.DataFrame([[self.name, self.status, now_time, self.message]], columns = ['name', 'status', 'update', 'message'])
        filepath = Path(ct.SPIDER_STATUS_FILE)
        if not filepath.exists():
            df.to_csv(filepath, index=False, header=True, mode='w', encoding='utf8')
        else:
            status_info = pd.read_csv(ct.SPIDER_STATUS_FILE)
            if self.name in status_info.name.tolist():
                status_info.loc[status_info.name == self.name] = [self.name, self.status, now_time, self.message]
                status_info.to_csv(ct.SPIDER_STATUS_FILE, index=False, header=True, mode="w", encoding='utf8')
            else:
                df.to_csv(filepath, index=False, header=False, mode='a+', encoding='utf8')
