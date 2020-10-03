# -*- coding: utf-8 -*-
# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy
class DspiderItem(scrapy.Item):
    # define the fields for your item here like:
    def convert(self, cstr, ctype = float):
        return ctype(0) if '-' == cstr else ctype(cstr.replace(',', ''))

    def convert_unit(self, cstr, unit, ctype = float):
        if cstr is None: return ctype(0)
        value = self.convert(cstr, ctype)
        if unit == '万': return round(value * 10000)
        return value

    def format_code(self, code, direction):
        if code == '-': return code
        if direction == 'south':
            return code.zfill(5)
        elif direction == 'north':
            return code.zfill(6)    

class MyDownloadItem(DspiderItem):
    file_urls = scrapy.Field()
    file_name = scrapy.Field()
 
class PlateValuationItem(DspiderItem):
    date = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    pe = scrapy.Field()
    ttm = scrapy.Field()
    pb = scrapy.Field()
    dividend = scrapy.Field()
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['code'], dc['name'], dc['pe'], dc['ttm'], dc['pb'], dc['dividend'])
        insert_sql = "insert into {}(date,code,name,pe,ttm,pb,dividend) VALUES (%s,%s,%s,%s,%s,%s,%s);".format(table)
        return insert_sql, params

class ChinaTreasuryRateItem(scrapy.Item):
    date   = scrapy.Field()
    name   = scrapy.Field()
    month3 = scrapy.Field()
    month6 = scrapy.Field()
    year1  = scrapy.Field()
    year3  = scrapy.Field()
    year5  = scrapy.Field()
    year7  = scrapy.Field()
    year10 = scrapy.Field()
    year30 = scrapy.Field()
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['name'], dc['month3'], dc['month6'], dc['year1'], dc['year3'], dc['year5'], dc['year7'], dc['year10'], dc['year30'])
        insert_sql = "insert into {}(date,name,month3,month6,year1,year3,year5,year7,year10,year30) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);".format(table)
        return insert_sql, params

    def empty(self):
        for key in ['month3', 'month6', 'year1', 'year3', 'year5', 'year7', 'year10', 'year30']:
            if self[key] is not None: return False
        return True

class ChinaSecurityIndustryValuationItem(PlateValuationItem):
    def empty(self):
        if (self['dividend'] == 0 and self['pb'] == 0 and self['ttm'] == 0 and self['pe'] == 0):
            return True
        return False

class SecurityExchangeCommissionValuationItem(ChinaSecurityIndustryValuationItem):
    pass

class SPledgeSituationItem(MyDownloadItem):
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
    def get_insert_sql(self, table):
        dc = dict(self)
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
    def empty(self):
        if self['code'] == '-' and self['name'] == '-':
            return True
        return False
        
    def get_insert_sql(self, table):
        if self.empty(): return None, None
        dc = dict(self)
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
    def empty(self):
        if (self['buy_trade_count'] == 0 and self['buy_turnover'] == 0
            and self['dqb'] == 0 and self['dqb_ratio'] == 0 and
            self['sell_trade_count'] == 0 and self['sell_turnover'] == 0 and
            self['total_trade_count'] == 0 and self['total_turnover'] == 0):
           return True
        return False

    def get_insert_sql(self, table):
        if self.empty(): return None, None
        dc = dict(self)
        params = (dc['date'], dc['total_turnover'], dc['buy_turnover'], dc['sell_turnover'], dc['total_trade_count'], dc['buy_trade_count'], dc['sell_trade_count'])
        insert_sql = "insert ignore into {}(date, total_turnover, buy_turnover, sell_turnover, total_trade_count, buy_trade_count, sell_trade_count) VALUES(%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params

class MonthInvestorSituationItem(DspiderItem):
    date                         = scrapy.Field()
    unit                         = scrapy.Field()
    new_investor                 = scrapy.Field()
    new_natural_person           = scrapy.Field()
    new_non_natural_person       = scrapy.Field()
    final_investor               = scrapy.Field()
    final_natural_person         = scrapy.Field()
    final_natural_a_person       = scrapy.Field()
    final_natural_b_person       = scrapy.Field()
    final_non_natural_person     = scrapy.Field()
    final_non_natural_a_person   = scrapy.Field()
    final_non_natural_b_person   = scrapy.Field()
    final_hold_investor          = scrapy.Field()
    final_a_hold_investor        = scrapy.Field()
    final_b_hold_investor        = scrapy.Field()
    trading_investor             = scrapy.Field()
    trading_a_investor           = scrapy.Field()
    trading_b_investor           = scrapy.Field()
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['new_investor'], dc['new_natural_person'], dc['new_non_natural_person'],
                  dc['final_investor'], dc['final_natural_person'], dc['final_natural_a_person'], dc['final_natural_b_person'], 
                  dc['final_non_natural_person'], dc['final_non_natural_a_person'], dc['final_non_natural_b_person'],
                  dc['final_hold_investor'], dc['final_a_hold_investor'], dc['final_b_hold_investor'],
                  dc['trading_investor'], dc['trading_a_investor'], dc['trading_b_investor'])
        insert_sql = "insert into {}(date, new_investor, new_natural_person, new_non_natural_person,\
                                    final_investor, final_natural_person, final_natural_a_person,\
                                    final_natural_b_person, final_non_natural_person, final_non_natural_a_person,\
                                    final_non_natural_b_person, final_hold_investor, final_a_hold_investor,\
                                    final_b_hold_investor, trading_investor, trading_a_investor, trading_b_investor) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);".format(table)
        return insert_sql, params

class StockFinancialDisclosureTimeItem(DspiderItem):
    code    = scrapy.Field()
    first   = scrapy.Field()
    changed = scrapy.Field()
    actual  = scrapy.Field()

class StockLimitItem(DspiderItem):
    date = scrapy.Field()
    code = scrapy.Field()
    price = scrapy.Field()
    pchange = scrapy.Field()
    prange = scrapy.Field()
    concept = scrapy.Field()
    fcb = scrapy.Field()
    flb = scrapy.Field()
    fdmoney = scrapy.Field()
    first_time = scrapy.Field()
    last_time = scrapy.Field()
    open_times = scrapy.Field()
    intensity = scrapy.Field()
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['code'], dc['price'], dc['pchange'], dc['prange'],\
              dc['concept'], dc['fcb'], dc['flb'], dc['fdmoney'], dc['first_time'],\
                                dc['last_time'], dc['open_times'], dc['intensity'])
        insert_sql = "insert ignore into {}(date, code, price, pchange, prange, concept, fcb, flb, fdmoney, first_time,last_time, open_times, intensity) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params

class MarginItem(DspiderItem):
    '''融资融券统计数据'''
    date = scrapy.Field()
    code = scrapy.Field()
    rzye = scrapy.Field()   #融资余额
    rzmre = scrapy.Field()  #融资买入额
    rzche = scrapy.Field()  #融资偿还额
    rqyl = scrapy.Field()   #融券余量
    rqye = scrapy.Field()   #融券余额
    rqmcl = scrapy.Field()  #融券卖出量
    rqchl = scrapy.Field()  #融券偿还量
    rzrqye = scrapy.Field() #融资融券余额
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['code'], dc['rzye'], dc['rzmre'], dc['rzche'], dc['rqye'], dc['rqyl'], dc['rqmcl'], dc['rqchl'], dc['rzrqye'])
        insert_sql = "insert ignore into {}(date, code, rzye, rzmre, rzche, rqye, rqyl, rqmcl, rqchl, rzrqye) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params

class BlockTradingItem(DspiderItem):
    '''大宗交易统计数据'''
    uid = scrapy.Field()
    date = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    volume = scrapy.Field()
    amount = scrapy.Field()
    branch_buy = scrapy.Field()
    branch_sell = scrapy.Field()
    def get_insert_sql(self, table):
        dc = dict(self)
        params = (dc['date'], dc['uid'], dc['code'], dc['name'], dc['price'], dc['volume'], dc['amount'], dc['branch_buy'], dc['branch_sell'])
        insert_sql = "insert ignore into {}(date, uid, code, name, price, volume, amount, branch_buy, branch_sell) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table)
        return insert_sql, params
