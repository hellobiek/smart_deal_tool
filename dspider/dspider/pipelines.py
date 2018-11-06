# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import poster, items
post_router={
    items.ShiborItem:poster.ShiborItemPoster,
    items.InvestorSituationItem:poster.InvestorSituationItemPoster,
    items.IndexCollectorItem:poster.IndexCollectorItemPoster,
    items.IndexStatisticItem:poster.IndexStatisticItemPoster,
    items.FoundationBriefItem:poster.FoundationBriefItemPoster
}

class DspiderPipeline(object):
    def process_item(self, item, spider):
        print(item)
        #post_router[item.__class__](item).post()
        return item
