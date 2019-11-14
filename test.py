# -*- coding: utf-8 -*-
import const as ct
from algotrade.model.follow_trend import FollowTrendModel
if __name__ == '__main__':
    #start_date = '2018-10-01'
    start_date = '2012-12-04'
    end_date   = '2019-11-01'
    redis_host = "127.0.0.1"
    dbinfo = ct.OUT_DB_INFO
    report_dir = "/Volumes/data/quant/stock/data/tdx/report"
    cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
    stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
    bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
    rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
    base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
    valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
    sci_val_file_path = "/Volumes/data/quant/crawler/china_security_industry_valuation/stock" 
    pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
    report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
    ftm = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo = dbinfo, redis_host = redis_host, should_create_mysqldb = True)
    base_df = ftm.stock_info_client.get()
    base_df = base_df[['code', 'name', 'timeToMarket', 'sw_industry']]
    leader = 1
    code = '000876'
    mdate = '2019-11-13'
    timeToMarket = base_df.loc[base_df.code == code, 'timeToMarket'].values[0]
    xx = ftm.compute_stock_pool(mdate)
    import pdb
    pdb.set_trace()
    xx = ftm.get_deleted_reason(code, mdate, timeToMarket, leader, False)
    print(xx)
