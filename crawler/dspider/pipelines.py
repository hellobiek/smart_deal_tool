# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from dspider import items
from scrapy import Request
import dspider.poster as poster
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

post_router = {
    items.HkexTradeTopTenItem:poster.HkexTradeTopTenItemPoster,
    items.HkexTradeOverviewItem:poster.HkexTradeOverviewPoster,
    items.SPledgeSituationItem:poster.SPledgeSituationItemPoster,
    items.InvestorSituationItem:poster.InvestorSituationItemPoster,
    items.MonthInvestorSituationItem:poster.MonthInvestorSituationItemPoster,
}

class DspiderPipeline(object):
    def process_item(self, item, spider):
        obj = post_router[item.__class__](item)
        if obj.check():
            obj.store()
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

    def item_completed(self, results, item, info):
        file_urls = [x['path'] for ok, x in results if ok]
        if not file_urls: raise DropItem("Item contains no files")
        item['file_urls'] = file_urls
        return item

    def file_path(self, request, response = None, info=None):
        return request.meta['item']['file_name'].replace('gpzyhgmx_', '')
