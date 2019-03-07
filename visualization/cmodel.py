#-*- coding: utf-8 -*-
import const as ct
import pandas as pd
from climit import CLimit
from cindex import CIndex
from rstock import RIndexStock
from common import get_day_nday_ago
from industry_info import IndustryInfo
from rindustry import RIndexIndustryInfo
from datamanager.margin import Margin
from datamanager.sexchange import StockExchange
from algotrade.selecters.anti_market_up import AntiMarketUpSelecter
from algotrade.selecters.market_oversold import MarketOversoldJudger
from algotrade.selecters.stronger_than_market import StrongerThanMarketSelecter
from algotrade.selecters.less_volume_in_high_profit import LowVolumeHighProfitSelecter
from algotrade.selecters.nei_chip_intensive import NeiChipIntensiveSelecter
from algotrade.selecters.no_chip_net_space import NoChipNetSpaceSelecter
from algotrade.selecters.bull_more_bear_less import BullMoreBearLessSelecter
from algotrade.selecters.game_kline_bigraise_and_large_volume import GameKLineBigraiseLargeVolumeSelecter
from algotrade.selecters.game_kline_bigraise_and_small_volume import GameKLineBigraiseSmallVolumeSelecter
def get_market_data(market, start_date, end_date):
    if market == ct.SH_MARKET_SYMBOL:
        sh_market_client = StockExchange(market = ct.SH_MARKET_SYMBOL)
        df = sh_market_client.get_k_data_in_range(start_date, end_date)
        df = df.loc[df.name == '上海市场']
    else:
        sz_market_client = StockExchange(market = ct.SZ_MARKET_SYMBOL)
        df = sz_market_client.get_k_data_in_range(start_date, end_date)
        df = df.loc[df.name == '深圳市场']
    df = df.round(2)
    df = df.drop_duplicates()
    df = df.reset_index(drop = True)
    df = df.sort_values(by = 'date', ascending= True)
    df.negotiable_value = (df.negotiable_value / 2).astype(int)
    return df

def get_index_data(cdate):
    df = pd.DataFrame()
    for code, name in ct.TDX_INDEX_DICT.items():
        data = CIndex(code).get_k_data(cdate)
        data['name'] = name
        data['code'] = code
        df = df.append(data)
    df = df.reset_index(drop = True)
    return df

def get_rzrq_info(market, start_date, end_date):
    margin_client = Margin()
    df = margin_client.get_k_data_in_range(start_date, end_date)
    if market == ct.SH_MARKET_SYMBOL:
        df = df.loc[df.code == 'SSE']
        df['code'] = '上海市场'
    else:
        df = df.loc[df.code == 'SZSE']
        df['code'] = '深圳市场'
    df = df.round(2)
    df['rzye']   = df['rzye']/1e+8
    df['rzmre']  = df['rzmre']/1e+8
    df['rzche']  = df['rzche']/1e+8
    df['rqye']   = df['rqye']/1e+8
    df['rzrqye'] = df['rzrqye']/1e+8
    df = df.drop_duplicates()
    df = df.reset_index(drop = True)
    df = df.sort_values(by = 'date', ascending= True)
    return df

def get_industry_data(cdate):
    ri = RIndexIndustryInfo()
    df = ri.get_k_data(cdate)
    if df.empty: return df
    df = df.reset_index(drop = True)
    df = df.sort_values(by = 'amount', ascending= False)
    df['money_change'] = (df['amount'] - df['preamount'])/1e8
    industry_info = IndustryInfo.get()
    df = pd.merge(df, industry_info, how='left', on=['code'])
    return df

def get_index_df(code, start_date, end_date):
    df = CIndex(code).get_k_data_in_range(start_date, end_date)
    df['time'] = df.index.tolist()
    df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
    return df

if __name__ == '__main__':
    cdate = '2019-01-23' 
    start_date = get_day_nday_ago(cdate, 100, dformat = "%Y-%m-%d")
    end_date = cdate
    #market info
    sh_df = get_market_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
    sz_df = get_market_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)
    date_list = list(set(sh_df.date.tolist()).intersection(set(sz_df.date.tolist())))
    sh_df = sh_df[sh_df.date.isin(date_list)]
    sz_df = sz_df[sz_df.date.isin(date_list)]
    #rzrq info
    sh_rzrq_df = get_rzrq_info(ct.SH_MARKET_SYMBOL, start_date, end_date)
    sz_rzrq_df = get_rzrq_info(ct.SZ_MARKET_SYMBOL, start_date, end_date)
    date_list = list(set(sh_rzrq_df.date.tolist()).intersection(set(sz_rzrq_df.date.tolist())))
    sh_rzrq_df = sh_rzrq_df[sh_rzrq_df.date.isin(date_list)]
    sz_rzrq_df = sz_rzrq_df[sz_rzrq_df.date.isin(date_list)]
    #average price info
    av_df = get_index_df('880003', start_date, end_date)
    #limit up and down info
    limit_info = CLimit().get_data(cdate)
    stock_info = RIndexStock().get_data(cdate)
    stock_info = stock_info[stock_info.volume > 0] #get volume > 0 stock list
    stock_info = stock_info.reset_index(drop = True)
    #index info
    index_info = get_index_data(end_date)
    #industry analysis
    industry_info = get_industry_data(cdate)
    #all stock info 
    all_stock_info = RIndexStock().get_k_data_in_range(start_date, end_date)

    stm = StrongerThanMarketSelecter()
    stm_code_list = stm.choose(all_stock_info, av_df)

    amus = AntiMarketUpSelecter()
    amus_code_list = amus.choose(stock_info)

    lvhps = LowVolumeHighProfitSelecter()
    lvhps_code_list = lvhps.choose(stock_info)

    gkblvs = GameKLineBigraiseLargeVolumeSelecter()
    gkblvs_code_list = gkblvs.choose(stock_info)

    gkbsvs = GameKLineBigraiseSmallVolumeSelecter()
    gkbsvs_code_list = gkbsvs.choose(stock_info)

    ncis = NeiChipIntensiveSelecter()
    ncis_code_list = ncis.choose(stock_info)

    bmbl = BullMoreBearLessSelecter()
    bmbl_code_list = bmbl.choose(all_stock_info)

    ncns = NoChipNetSpaceSelecter()
    ncns_code_list = ncns.choose(stock_info)

    moj = MarketOversoldJudger()
    is_over_sold = moj.judge(stock_info)
