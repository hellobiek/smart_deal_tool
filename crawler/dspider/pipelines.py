# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from dspider import items
from scrapy import Request
import dspider.poster as poster
from scrapy.pipelines.files import FilesPipeline

post_router={
    items.SPledgeSituationItem:poster.SPledgeSituationItemPoster,
    items.InvestorSituationItem:poster.InvestorSituationItemPoster,
    items.HkexTradeOverviewItem:poster.HkexTradeOverviewPoster,
    items.HkexTradeTopTenItem:poster.HkexTradeTopTenItemPoster,
}

class DspiderPipeline(object):
    def process_item(self, item, spider):
        post_router[item.__class__](item).store()
        return item

class SPledgePipline(FilesPipeline):
    """
    继承FilesPipeline，更改其存储文件的方式
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_media_requests(self, item, info):
        # FilesPepeline 根据file_urls指定的url进行爬取，该方法为每个url生成一个Request后 Return 
        for file_url in item['file_urls']:
            yield Request(file_url, meta={'item': item})

    def file_path(self, request, response = None, info=None):
        return request.meta['item']['file_name'].replace('gpzyhgmx_', '')
