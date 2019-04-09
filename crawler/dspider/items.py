# -*- coding: utf-8 -*-
# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy
from datetime import datetime
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

class HkexTradeTopTenItem(DspiderItem):
    market = scrapy.Field()
    direction = scrapy.Field()
    date = scrapy.Field()
    rank = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    buy_turnover = scrapy.Field()
    sell_turnover = scrapy.Field()
    total_turnover = scrapy.Field()
    def convert(self):
        res = {}
        dc = dict(self)
        ks = ['total_turnover', 'buy_turnover', 'sell_turnover']
        if dc['code'] == '-': return None
        for k in ks:
            dc[k] = 0 if '-' == dc[k] else float(dc[k].replace(',',''))
        return dc

    def get_insert_sql(self, table):
        dc = self.convert()
        if dc is None: return None, None
        params = (dc['date'], dc['rank'], dc['code'], dc['name'], dc['total_turnover'], dc['buy_turnover'], dc['sell_turnover'])
        insert_sql = "insert ignore into {}(date, rank, code, name, total_turnover, buy_turnover, sell_turnover) VALUES (%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params

class HkexTradeOverviewItem(DspiderItem):
    market = scrapy.Field()
    direction = scrapy.Field()
    date = scrapy.Field()
    total_turnover = scrapy.Field()
    buy_turnover = scrapy.Field()
    sell_turnover = scrapy.Field()
    total_trade_count = scrapy.Field()
    buy_trade_count = scrapy.Field()
    sell_trade_count = scrapy.Field()
    dqb = scrapy.Field()
    dqb_ratio = scrapy.Field()
    def convert(self):
        res = {}
        dc = dict(self)
        ks = ['total_turnover', 'buy_turnover', 'sell_turnover', 'total_trade_count', 'buy_trade_count', 'sell_trade_count']
        for k in ks:
            dc[k] = 0 if '-' == dc[k] else float(dc[k].replace(',',''))
        total = 0
        for k in ks: total += dc[k]
        return None if 0 == total else dc

    def get_insert_sql(self, table):
        dc = self.convert()
        if dc is None: return None, None
        params = (dc['date'], dc['total_turnover'], dc['buy_turnover'], dc['sell_turnover'], dc['total_trade_count'], dc['buy_trade_count'], dc['sell_trade_count'])
        insert_sql = "insert ignore into {}(date, total_turnover, buy_turnover, sell_turnover, total_trade_count, buy_trade_count, sell_trade_count) VALUES(%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params
