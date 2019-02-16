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

class InvestorSituationItem(DspiderItem):
    push_date                = scrapy.Field()
    new_investor             = scrapy.Field()
    final_investor           = scrapy.Field()
    new_natural_person       = scrapy.Field()
    new_non_natural_person   = scrapy.Field()
    final_natural_person     = scrapy.Field()
    final_non_natural_person = scrapy.Field()
    unit                     = scrapy.Field()
    def convert(self):
        dc = dict(self)
        #定义要做数据类型转换的k值，把str 转换成float
        res = {}
        ks = ['new_investor','final_investor','new_natural_person','new_non_natural_person','final_natural_person','final_non_natural_person']
        if '万' in dc['unit']:
            for k in ks:
                res[k] = float(dc[k].replace(',','')) * 10000
            res['date'] = dc['push_date']
        return res

    def get_insert_sql(self, table):
        dc = self.convert()
        params = (dc['date'], dc['new_investor'], dc['final_investor'], dc['new_natural_person'], dc['new_non_natural_person'], dc['final_natural_person'], dc['final_non_natural_person'])
        insert_sql = "insert into {}(date,new_investor,final_investor,new_natural_person,new_non_natural_person,final_natural_person,final_non_natural_person) VALUES (%s,%s,%s,%s,%s,%s,%s);".format(table)
        return insert_sql, params
