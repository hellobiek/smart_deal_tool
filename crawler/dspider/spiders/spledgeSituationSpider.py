# -*- coding: utf-8 -*-
import datetime
from datetime import datetime
from scrapy import FormRequest
from dspider.myspider import BasicSpider
from dspider.items import SPledgeSituationItem
class SPledgeSituationSpider(BasicSpider):
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
        start_date = self.get_nday_ago(end_date, 60, dformat = '%Y.%m.%d')
        while start_date < end_date:
            start_date = self.get_next_date(sdate = start_date)
            formdata['queryDate'] = start_date
            yield FormRequest(url = self.start_urls[0], method = 'GET', formdata = formdata, callback = self.parse)

    def parse(self, response):
        fname = response.headers['Content-Disposition'].decode().split('=')[1]
        yield SPledgeSituationItem(file_urls = [response.url], file_name = fname)
