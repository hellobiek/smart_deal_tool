#coding=utf-8
from schematics.models import Model
from schematics.types import URLType, StringType, ListType, FloatType, IntType

class MonthInvestorSituationModel(Model):
    date = StringType(required=True)
    unit = StringType(required=True)
    new_investor = FloatType(required=True)
    new_natural_person = FloatType(required=True)
    new_non_natural_person = FloatType(required=True)
    final_investor = FloatType(required=True)
    final_natural_person = FloatType(required=True)
    final_natural_a_person = FloatType(required=True)
    final_natural_b_person = FloatType(required=True)
    final_non_natural_person = FloatType(required=True)
    final_non_natural_a_person = FloatType(required=True)
    final_non_natural_b_person = FloatType(required=True)
    final_hold_investor = FloatType(required=True)
    final_a_hold_investor = FloatType(required=True)
    final_b_hold_investor = FloatType(required=True)
    trading_investor = FloatType(required=True)
    trading_a_investor = FloatType(required=True)
    trading_b_investor = FloatType(required=True)

class PlateValuationModel(Model):
    date = StringType(required=True)
    code = StringType(required=True)
    name = StringType(required=True)
    pe = FloatType(required=True)
    ttm = FloatType(required=True)
    pb = FloatType(required=True)
    dividend = FloatType(required=True)

class ChinaTreasuryRateModel(Model):
    date = StringType(required=True)
    name = StringType(required=True)
    month3 = FloatType(required=True)
    month6 = FloatType(required=True)
    year1 = FloatType(required=True)
    year3 = FloatType(required=True)
    year5 = FloatType(required=True)
    year7 = FloatType(required=True)
    year10 = FloatType(required=True)
    year30 = FloatType(required=True)
    
class InvestorSituationModel(Model):
    date = StringType(required=True)
    new_investor = FloatType(required=True)
    final_investor = FloatType(required=True)
    new_natural_person = FloatType(required=True)
    new_non_natural_person = FloatType(required=True)
    final_natural_person = FloatType(required=True)
    final_non_natural_person = FloatType(required=True)
    unit = StringType(required=True)

class HkexTradeOverviewModel(Model):
    market = StringType(required=True)
    direction = StringType(required=True)
    date = StringType(required=True)
    total_turnover = FloatType(required=True)
    buy_turnover = FloatType(required=True)
    sell_turnover = FloatType(required=True)
    total_trade_count = IntType(required=True)
    buy_trade_count = IntType(required=True)
    sell_trade_count = IntType(required=True)
    dqb = FloatType(required=True)
    dqb_ratio = FloatType(required=True)

class HkexTradeTopTenModel(Model):
    market = StringType(required=True)
    direction = StringType(required=True)
    date = StringType(required=True)
    rank = IntType(required=True)
    code = StringType(required=True)
    name = StringType(required=True)
    buy_turnover = FloatType(required=True)
    sell_turnover = FloatType(required=True)
    total_turnover = FloatType(required=True)
