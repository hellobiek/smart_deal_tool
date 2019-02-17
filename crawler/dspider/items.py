# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy
from dspider.utils import datetime_to_str

class DspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class SPledgeSituationItem(DspiderItem):
    files = scrapy.Field()
    file_urls = scrapy.Field()
    file_name = scrapy.Field()

class InvestorSituationItem(DspiderItem):
    date                     = scrapy.Field()
    new_investor             = scrapy.Field()
    final_investor           = scrapy.Field()
    new_natural_person       = scrapy.Field()
    new_non_natural_person   = scrapy.Field()
    final_natural_person     = scrapy.Field()
    final_non_natural_person = scrapy.Field()
    unit                     = scrapy.Field()
    def convert(self):
        res = {}
        dc = dict(self)
        ks = ['new_investor','final_investor','new_natural_person','new_non_natural_person','final_natural_person','final_non_natural_person']
        for k in ks:
            if '-' == dc[k]: dc[k] = '0'
        if '万' in dc['unit']:
            for k in ks:
                res[k] = float(dc[k].replace(',','')) * 10000
        res['date'] = dc['date']
        return res

    def get_insert_sql(self, table):
        dc = self.convert()
        params = (dc['date'], dc['new_investor'], dc['final_investor'], dc['new_natural_person'], dc['new_non_natural_person'], dc['final_natural_person'], dc['final_non_natural_person'])
        insert_sql = "insert into {}(date,new_investor,final_investor,new_natural_person,new_non_natural_person,final_natural_person,final_non_natural_person) VALUES (%s,%s,%s,%s,%s,%s,%s);".format(table)
        return insert_sql, params
