# -*- coding: utf-8 -*-
import time
import datetime
import calendar
from scrapy import Spider, FormRequest
from datetime import datetime, timedelta
from dspider.items import SPledgeSituationItem
class SPledgeSituationSpider(Spider):
    name = 'spledgeSituationSpider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'dspider.pipelines.SPledgePipline': 1
        }
    }
    allowed_domains = ['www.chinaclear.cn']
    start_urls = ['http://www.chinaclear.cn/cms-rank/downloadFile']
    def start_requests(self):
        formdata = dict()
        formdata['queryDate'] = ''
        formdata['type'] = 'proportion'
        end_date = datetime.now().strftime('%Y.%m.%d')
        start_date = self.get_nday_ago(end_date, 30, dformat = '%Y.%m.%d')
        while start_date < end_date:
            start_date = self.get_next_date(sdate = start_date)
            formdata['queryDate'] = start_date
            yield FormRequest(url = self.start_urls[0], method = 'GET', formdata = formdata, callback = self.parse)

    def parse(self, response): 
        fname = response.headers['Content-Disposition'].decode().split('=')[1]
        yield SPledgeSituationItem(file_urls = [response.url], file_name = fname)

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
