# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import poster, items
post_router = {
    items.InvestorSituationItem:poster.InvestorSituationItemPoster
}

class DspiderPipeline(object):
    def process_item(self, item, spider):
        post_router[item.__class__](item).post()
        return item
