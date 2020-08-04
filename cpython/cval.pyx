# -*- coding: utf-8 -*-
# cython: language_level=3, wraparound=False, boundscheck=False, nonecheck=False, infer_types=True
import os
import xlrd
import calendar
import traceback
import const as ct
import numpy as np
cimport numpy as np
import pandas as pd
from pandas import Series
from cindex import CIndex
from cstock import CStock
from pathlib import Path
from pandas import DataFrame
from base.clog import getLogger
from datamanager.cbonus import CBonus
from datamanager.creport import CReport
from datetime import datetime, timedelta
from base.cdate import quarter, report_date_with, int_to_datetime, prev_report_date_with, get_pre_date, get_next_date, pre_report_date_with
DTYPE_DICT = {'date': int,
              'code': str, 
              'bps': float, #每股净资产(book value per share)
              'eps': float, #基本每股收益(earnings per share)
              'npbo': float, #归属于母公司所有者的净利润(net profit belonging to owner)
              'ccs': float, #已上市流通A股(circulating capital share) 
              'tcs': float, #总股本(total capital share)
              'roa': float, #净资产回报率(roa)
              'mf': float, #货币资金(money funds)
              'stb': float, #短期借款(short term borrowing)
              'ar': float, #应收账款(accounts receivable)
              'br': float, #应收票据(bill receivable)
              'ta': float, #资产总计(total assets)
              'fa': float, #固定资产(fixed assets)
              'ca': float, #流动资产合计(current assets)
              'pp': float, #应付职工薪酬(payroll payable)
              'npm': float, #销售净利率(net profit margin)
              'gpr': float, #销售毛利率(gross profit ratio)
              'dar': float, #资产负债率(debt asset ratio)
              'iar': float, #存货比率(inventory asset ratio)
              'cfpsfo': float, #每股经营性现金(cash flow per share from operations)
              'micc': float, #营业收入现金含量(main income cash content)
              'crr': float, #全部资产现金回收率(cash recovery rate of all assets)
              'ncf': float, #经营活动现金净流量与净利润比率(net cash flow to net profit ratio)
              'cip': float, #在建工程(construction in process)
              'igr': float, #营业收入增长率(increase growth rate)
              'ngr': float, #净利润增长率(net growth rate)
              'ncdr': float, #非流动负债比率(non-current debt ratio)
              'cdr': float, #流动负债比率(current debt ratio)
              'cdbr': float, #现金到期债务比率(cash due debt ratio)
              'goodwill': float, #商誉(goodwill)
              'revenue': float, #营业收入(revenue)
              'inventory': float, #存货(inventory)
              'np': float, #净利润(net profit)
              'rnp': float, #扣除非经常性损益后的净利润(recurrent net profit)
              'rroe': float, #加权净资产收益率(rroe)
              't10n': float, #十大股东持股数量合计(top 10 stock holder num)
              'nth': float, #国家队持股(national team holdings)
              'largest_holding': float, #第一大股东的持股数量
              'institution_holders': int, #机构总数
              'institution_holding': float, #机构持股数量
              'qfii_holders': int, #QFII机构数
              'qfii_holding': float, #QFII持股量
              'social_security_holders': int, #社保机构数
              'social_security_holding': float, #社保持股量
              'broker_holders': int, #券商机构数
              'broker_holding': float, #券商持股量
              'insurance_holders': int, #保险机构数
              'insurance_holding': float, #保险持股量
              'annuity_holders': int, #年金机构数
              'annuity_holding': float, #年金持股量
              'fund_holders': int, #基金机构数
              'fund_holding': float, #基金持股量
              'private_holders': int, #私募机构数
              'private_holding': float, #私募持股量
              'financial_company_holders': int, #财务公司机构数
              'financial_company_holding': float, #财务公司持股量
              'publish': int}

cdef str PRE_CUR_CODE = '', PRE_YEAR_CODE = ''
cdef dict PRE_YEAR_ITEM = dict(), PRE_CUR_ITEM = dict()
cdef int PRE_YEAR_REPORT_DATE = 0, PRE_CUR_REPORT_DATE = 0
cdef class CValuation(object):
    cdef public np.ndarray valuation_data
    cdef public str report_data_path, rvaluation_dir, pledge_file_dir
    cdef public object logger, bonus_client, report_client
    def __init__(self, str valution_path = ct.VALUATION_PATH, str bonus_path = ct.BONUS_PATH, 
            str report_dir = ct.REPORT_DIR, str report_publish_dir = ct.REPORT_PUBLISH_DIR, 
            str pledge_file_dir = ct.PLEDGE_FILE_DIR, str rvaluation_dir = ct.RVALUATION_DIR, needUpdate = False):
        self.logger = getLogger(__name__)
        self.rvaluation_dir = rvaluation_dir
        self.pledge_file_dir = pledge_file_dir
        self.bonus_client = CBonus(bonus_path)
        self.report_client = CReport(report_dir, report_publish_dir)
        self.report_data_path = valution_path
        if needUpdate: self.convert()
        self.valuation_data = self.get_reports_data()

    cdef object get_reports_data(self):
        cdef object df
        if not Path(self.report_data_path).exists(): return None 
        df = pd.read_csv(self.report_data_path, header = 0, encoding = "utf8", usecols = DTYPE_DICT.keys(), dtype = DTYPE_DICT)
        df = df.drop_duplicates(subset=['date','code'])
        df = df.reset_index(drop = True)
        return df.to_records(index = False)

    def convert(self):
        #date, code, 1.基本每股收益(earnings per share)、2.扣非每股收益(non-earnings per share)、
        #4.每股净资产(book value per share)、6.净资产回报率(roa)、72.所有者权益（或股东权益）合计(net assert)、
        #96.归属于母公司所有者的净利润(net profit belonging to owner)、238.总股本(total capital share)、239.已上市流通A股(circulating capital share)、
        #8.货币资金(money funds)、10.应收票据(bill receivable)、11.应收账款(accounts receivable)、12.预付款项(prepayments)、
        #13.其他应收款(other receivables)、14.应收关联公司款(receivables from related companies)、
        #172.应收帐款周转率(receivables turnover ratio)、177.应收帐款周转天数(days sales outstanding)、
        #17.存货(inventory)、40.资产总计(total assets)、41.短期借款(short term borrowing)、43.应付票据(bills payable)、
        #44.应付账款(accounts payable)、45.预收款项(accounts received in advance)、46.应付职工薪酬(payroll payable)、
        #47.应交税费(taxes and dues payable)、48.应付利息(accrued interest payable)、51.应付关联公司款(coping with affiliates)、
        #55.长期借款(long term loan)、63.负债合计(total liabilities)、77.销售费用(selling expenses)、78.管理费用(managing costs)、
        #79.勘探费用(exploration cost)、80.财务费用(financing costs)、173.存货周转率(inventory turnover)、
        #178.存货周转天数(days sales of inventory)、210.资产负债率(debt asset ratio)、213.存货比率(inventory asset ratio)、
        #28.在建工程(construction in process)、34.开发支出(development expenditure)、230.营业收入(revenue)、
        #231.营业利润(operating profit ratio)、35.商誉(goodwill)、242.股东人数(holders)、
        #243.第一大股东的持股数量、246.机构总量（家）、247.机构持股总量(股)、248.QFII机构数、249.QFII持股量、250.券商机构数、251.券商持股量、
        #252.保险机构数、253.保险持股量、254.基金机构数、255.基金持股量
        #256.社保机构数、257.社保持股量、258.私募机构数、259.私募持股量
        #260.财务公司机构数、261.财务公司持股量、262.年金机构数、263.年金持股量
        #183.营业收入增长率(increase growth rate)、184.净利润增长率(net growth rate)、
        #189.营业利润增长率(profit growth rate)、191.扣非净利润同比(non-net profit rate)
        #21.流动资产合计(current assets)、27.固定资产(fixed assets)、199.销售净利率(net profit margin)、
        #202.销售毛利率(gross profit ratio)、225.每股现金流量净额(cash flow per share)
        #220.营业收入现金含量(main income cash content)、229.全部资产现金回收率(cash recovery rate of all assets)、
        #228.经营活动现金净流量与净利润比率(net cash flow to net profit ratio)、159.流动比率(working capital ratio)、
        #160.速动比率(quick ratio)、161.现金比率(currency ratio)、219.每股经营性现金(cash flow per share from operations)
        #163.非流动负债比率(non-current debt ratio)、#164.流动负债比率(current debt ratio)、#165.现金到期债务比率(cash due debt ratio)、
        #95.净利润(net profit)、232.扣除非经常性损益后的净利润(recurrent net profit)、264.十大流通股东持A股数量合计(top 10 stock holder num)、
        #281.加权净资产收益率(rroe)、
        #284.国家队持股数量(national team holdings)（万股)[注：本指标统计包含汇金公司、证金公司、外汇管理局旗下投资平台、国家队基金、国开、养老金以及中科汇通等国家队机构持股数量]、
        #财报披露时间(publish)
        mcols = ['date','code',
                 'eps', #基本每股收益(earnings per share)
                 'neps', #扣非每股收益(non-earnings per share)
                 'bps', #每股净资产(book value per share)
                 'roa', #净资产回报率(roa)
                 'na', #所有者权益(net assert)
                 'npbo', #归属于母公司所有者的净利润(net profit belonging to owner)
                 'tcs', #总股本(total capital share)
                 'ccs', #已上市流通A股(circulating capital share)
                 'mf', #货币资金(money funds)
                 'br', #应收票据(bill receivable)
                 'ar', #应收账款(accounts receivable)
                 'prepayments', #预付款项(prepayments)
                 'or', #其他应收款(other receivables)
                 'rfrc', #应收关联公司款(receivables from related companies)
                 'rtr', #应收帐款周转率(receivables turnover ratio)
                 'dso', #days sales outstanding
                 'inventory',#存货(inventory)
                 'ta', #total assets
                 'stb', #短期借款(short term borrowing)
                 'bp', #bills payable
                 'ap', #accounts payable
                 'aria', #accounts received in advance
                 'pp', #应付职工薪酬(payroll payable)
                 'tadp', #taxes and dues payable
                 'aip', #accrued interest payable
                 'cwa', #coping with affiliates
                 'ltl', #long term loan
                 'tl', #total liabilities
                 'se', #selling expenses
                 'mc', #managing costs
                 'ec', #exploration cost
                 'fc', #financing costs
                 'it', #inventory turnover
                 'dsoi', #days sales of inventory
                 'dar', #资产负债率(debt asset ratio)
                 'iar', #存货比率(inventory asset ratio)
                 'cip', #在建工程(construction in process)
                 'de', #development expenditure
                 'revenue',#营业收入(revenue)
                 'opr', #营业利润(operating profit ratio)
                 'goodwill', #商誉(goodwill)
                 'holders',
                 'largest_holding', #第一大股东的持股数量
                 'institution_holders', #机构总数
                 'institution_holding', #机构持股数量
                 'qfii_holders', #QFII机构数
                 'qfii_holding', #QFII持股量
                 'broker_holders', #券商机构数
                 'broker_holding', #券商持股量
                 'insurance_holders', #保险机构数
                 'insurance_holding', #保险持股量
                 'fund_holders', #基金机构数
                 'fund_holding', #基金持股量
                 'social_security_holders', #社保机构数
                 'social_security_holding', #社保持股量
                 'private_holders', #私募机构数
                 'private_holding', #私募持股量
                 'financial_company_holders', #财务公司机构数
                 'financial_company_holding', #财务公司持股量
                 'annuity_holders', #年金机构数
                 'annuity_holding', #年金持股量
                 'igr', #营业收入增长率(increase growth rate)
                 'ngr', #净利润增长率(net growth rate)
                 'pgr', #营业利润增长率(profit growth rate)
                 'npr', #扣非净利润同比(non-net profit rate)
                 'ca', #流动资产合计(current assets)
                 'fa', #固定资产(fixed assets)
                 'npm', #销售净利率(net profit margin)
                 'gpr', #销售毛利率(gross profit ratio)
                 'cfps', #每股现金流量净额(cash flow per share)
                 'micc', #营业收入现金含量(main income cash content)
                 'crr', #全部资产现金回收率(cash recovery rate of all assets)
                 'ncf', #经营活动现金净流量与净利润比率(net cash flow to net profit ratio)
                 'wcr', #流动比率(working capital ratio)
                 'qr', #速动比率(quick ratio)
                 'cr', #现金比率(currency ratio)
                 'cfpsfo', #每股经营性现金(cash flow per share from operations)
                 'ncdr', #非流动负债比率(non-current debt ratio)
                 'cdr', #流动负债比率(current debt ratio)
                 'cdbr', #现金到期债务比率(cash due debt ratio)
                 'np', #净利润(net profit)
                 'rnp', #扣除非经常性损益后的净利润(recurrent net profit)
                 't10n', #十大流通股东持A股数量合计(top 10 stock holder num)
                 'rroe', #加权净资产收益率(rroe)
                 'nth', #国家队持股数量(national team holdings)
                 'publish']
        date_list = self.report_client.get_all_report_list()
        is_first = True
        prefix = "col%s"
        for mdate in date_list:
            report_list = list()
            report_df = self.report_client.get_report_data(mdate)
            for idx, row in report_df.iterrows():
                rroe = 0 if prefix%281 not in row else row[prefix%281]
                nth = 0 if prefix%284 not in row else row[prefix%284]
                top10holdings = 0 if prefix%264 not in row else row[prefix%264]
                pdate = self.report_client.get_report_publish_time(row['date'], row['code'])
                report_list.append([row['date'], row['code'], row[prefix%1],  row[prefix%2], row[prefix%4], 
                            row[prefix%6],  row[prefix%72],  row[prefix%96],  row[prefix%238], row[prefix%239],
                            row[prefix%8],  row[prefix%10],  row[prefix%11],  row[prefix%12], row[prefix%13],
                            row[prefix%14], row[prefix%172], row[prefix%177], row[prefix%17], row[prefix%40],
                            row[prefix%41], row[prefix%43], row[prefix%44], row[prefix%45], row[prefix%46],
                            row[prefix%47], row[prefix%48], row[prefix%51], row[prefix%55], row[prefix%63],
                            row[prefix%77], row[prefix%78], row[prefix%79], row[prefix%80], row[prefix%173],
                            row[prefix%178], row[prefix%210], row[prefix%213], row[prefix%28], row[prefix%34],
                            row[prefix%230], row[prefix%231], row[prefix%35], row[prefix%242], row[prefix%243],
                            row[prefix%246], row[prefix%247], row[prefix%248], row[prefix%249], row[prefix%250],
                            row[prefix%251], row[prefix%252], row[prefix%253], row[prefix%254], row[prefix%255],
                            row[prefix%256], row[prefix%257], row[prefix%258], row[prefix%259], row[prefix%260],
                            row[prefix%261], row[prefix%262], row[prefix%263], row[prefix%183], row[prefix%184],
                            row[prefix%189], row[prefix%191], row[prefix%21], row[prefix%27], row[prefix%199], 
                            row[prefix%202], row[prefix%225], row[prefix%220], row[prefix%229], row[prefix%228],
                            row[prefix%159], row[prefix%160], row[prefix%161], row[prefix%219], row[prefix%163],
                            row[prefix%164], row[prefix%165], row[prefix%95], row[prefix%232], top10holdings, rroe, nth, pdate])
            result_df = DataFrame(report_list, columns = mcols)
            result_df = result_df.sort_values(['code'], ascending = 1)
            result_df['code'] = result_df['code'].map(lambda x: str(x).zfill(6))
            if is_first is True:
                is_first = False
                result_df.to_csv(self.report_data_path, index=False, header=True, mode='w', encoding='utf8')
            else:
                result_df.to_csv(self.report_data_path, index=False, header=False, mode='a+', encoding='utf8')

    cdef (float, float) get_css_tcs(self, str code, int tdate, dict item):
        cdef float css, tcs
        if len(item) > 0:
            ccs, tcs = item["ccs"], item["tcs"] #流通股, 总股本
            if ccs == 0 or tcs == 0:
                self.logger.debug("get code:{}, tdate:{} failed. ccs:{}, tcs:{}".format(code, tdate, ccs, tcs))
                ccs, tcs = self.bonus_client.get_css_tcs(code, tdate)
                if ccs == 0 or tcs == 0:
                    self.logger.error("unexpected css tcs code:{}, tdate:{} for item is not None".format(code, tdate))
        else:
            ccs, tcs = self.bonus_client.get_css_tcs(code, tdate)
            if ccs == 0 or tcs == 0:
                self.logger.error("unexpected css tcs code:{}, tdate:{} for item is None".format(code, tdate))
        return ccs, tcs

    cdef (float, float) get_css_tcs_mv(self, float close, float ccs, float tcs):
        return close * ccs, close * tcs

    def set_stock_valuation(self, dict code2time_dict, str mdate, str code):
        cdef list data
        cdef dict year_item, cur_item
        cdef object df, vdf, stock_obj
        cdef int timeToMarket = code2time_dict[code]
        cdef float pe_value, ttm_value, pb_value, roe_value, ccs, tcs, ccs_mv, tcs_mv, dividend_value
        def compute(int tdate, float close):
            if tdate > 20040101:
                year_item = self.get_year_report_item(tdate, code, timeToMarket)
                cur_item = self.get_actual_report_item(tdate, code, timeToMarket)
                if len(year_item) > 0 and len(cur_item) > 0:
                    if year_item['publish'] > cur_item['publish']:
                        #年报比当前的财报公布的还晚
                        self.logger.error("code:{}, tdate:{}, year report publish date:{}, cur report publish date:{}".format(code, tdate, year_item['publish'], cur_item['publish']))
                        return tdate, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                pe_value = self.pe(cur_item, year_item, close)
                ttm_value = self.ttm(cur_item, code, close)
                pb_value = self.pb(cur_item, close)
                roe_value = pb_value / ttm_value if ttm_value != 0.0 else 0.0
                ccs, tcs = self.get_css_tcs(code, tdate, cur_item)
                ccs_mv, tcs_mv = self.get_css_tcs_mv(close, ccs, tcs)
                dividend_value = self.bonus_client.get_dividend_rate(tdate, code, close)
                return tdate, pe_value, ttm_value, pb_value, roe_value, dividend_value, ccs, tcs, ccs_mv, tcs_mv
            else:
                return tdate, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        stock_obj = CStock(code)
        df = stock_obj.read()[0] if mdate == '' else stock_obj.read(cdate = mdate)[0]
        if df.empty: return (code, True)
        df = df.reset_index(drop = True)
        vfunc = np.vectorize(compute)
        data = [item for item in zip(*vfunc(df['date'].values, df['close'].values))]
        vdf = DataFrame(data, columns=["date", "pe", "ttm", "pb", "roe", "dr", "ccs", "tcs", "ccs_mv", "tcs_mv"])
        vdf['code'] = code
        return (code, stock_obj.set_val_data(vdf, mdate))

    def update_vertical_data(self, object df, list dtype_list, int mdate):
        cdef str dtype
        cdef dict item
        cdef object dval
        mdate = pre_report_date_with(mdate)
        def cfunc(str code, int time2Market):
            item = self.get_report_item(mdate, code)
            if 1 == len(dtype_list):
                return item[dtype_list[0]] if item else None
            else:
                return tuple([item[dtype] for dtype in dtype_list]) if item else tuple([None for dtype in dtype_list])
        vfunc = np.vectorize(cfunc)
        with pd.option_context('mode.chained_assignment', None):
            if len(dtype_list) == 1:
                df[dtype_list[0]] = vfunc(df['code'].values, df['timeToMarket'].values)
            else:
                for dtype, dval in zip(dtype_list, vfunc(df['code'].values, df['timeToMarket'].values)):
                    df[dtype] = dval

    cpdef object get_horizontal_data(self, str code):
        return self.get_report_items(code)

    cdef object get_report_items(self, str code):
        cdef object data = self.valuation_data[np.where(self.valuation_data["code"] == code)]
        np_data = data[data['date'].argsort()]
        return DataFrame(data = np_data)

    cdef dict get_report_item(self, int mdate, str code):
        cdef object data_ = self.valuation_data[np.where((self.valuation_data["date"] == mdate) & (self.valuation_data["code"] == code))]
        return dict() if len(data_) == 0 else {name:data_[name].item() for name in data_.dtype.names}

    cdef dict get_year_report_item(self, int mdate, str code, int timeToMarket):
        cdef int report_date
        cdef dict report_item
        global PRE_YEAR_CODE, PRE_YEAR_REPORT_DATE, PRE_YEAR_ITEM
        curday = int_to_datetime(mdate)
        report_date = int("%d1231" % (curday.year - 1))

        if code == PRE_YEAR_CODE and report_date == PRE_YEAR_REPORT_DATE: return PRE_YEAR_ITEM

        PRE_YEAR_CODE = code
        PRE_YEAR_REPORT_DATE = report_date

        if timeToMarket > report_date:
            PRE_YEAR_ITEM = dict()
            return PRE_YEAR_ITEM

        #上市时间早于标准年报更新时间
        report_item = self.get_report_item(report_date, code)
        if 0 == len(report_item):
            #获取不到年报
            self.logger.debug("{} year report is empty for {}".format(report_date, code))
        else:
            if report_item["publish"] > mdate:
                #年报实际公布时间晚于当前时间
                report_date = int("%d1231" % (curday.year - 2))
                if timeToMarket > report_date:
                    PRE_YEAR_ITEM = dict()
                    return PRE_YEAR_ITEM
                report_item = self.get_report_item(report_date, code)
                if 0 == len(report_item):
                    self.logger.debug("get 2 years before report error., code:{}, date:{}".format(code, mdate))
        PRE_YEAR_ITEM = report_item
        return PRE_YEAR_ITEM

    cdef float pb(self, dict item, float price):
        """
        获取某只股票某个时段的PB值
        :param mdate:
        :param code:
        :param price:
        :return:
        """
        return 0.0 if len(item) == 0 or item['bps'] == 0 else price / item['bps']

    cdef float pe(self, dict cur_item, dict year_item, float price):
        """
        获取某只股票某个时段的静态市盈率
        """
        cdef float lyr_eps
        if len(cur_item) == 0  or cur_item['tcs'] == 0: return 0.0
        if len(year_item) == 0 : return 0.0
        # 用年报的每股收益 * 因股本变动导致的稀释
        lyr_eps = (year_item['eps'] * year_item['tcs']) / cur_item['tcs']
        return 0.0 if lyr_eps == 0 else price / lyr_eps

    cdef float ttm(self, dict cur_item, str code, float price):
        """
        获取指定日志的滚动市盈率(从2003年开始计算)
        :param date:
        :param code:
        :param price:
        :return:
        """
        cdef float current_eps
        cdef int year, report_quarter
        cdef dict year_report, q3_report, q2_report, q1_report 
        if len(cur_item) == 0 or cur_item['tcs'] == 0.0:
            #刚上市还没公布最新财报或者没有股本信息
            return 0.0

        (year, report_quarter) = quarter(cur_item['date'])
        if report_quarter == 3:
            # 刚公布了年报,直接以年报计算
            return 0.0 if cur_item['eps'] == 0 else price / cur_item['eps']
    
        if report_quarter == 2:
            # 当前是三季报, 还需要 上一年年报 - 上一年三季报 + 当前的三季报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q3_report = self.get_report_item(int("%d0930" % (year-1)), code)
            if len(year_report) == 0 or len(q3_report) == 0:
                # 上市不足一年
                if len(year_report) == 0:
                    self.logger.debug("report_quarter == 2, code:{}, year_report:{} is None".format(code, "%d1231" % (year-1)))
                else:
                    self.logger.debug("report_quarter == 2, code:{}, q3_report:{} is None".format(code, "%d0930" % (year-1)))
                return (price * cur_item['tcs'])/ cur_item['npbo']
    
            current_eps = (year_report['npbo'] - q3_report['npbo'] + cur_item['npbo']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
    
        if report_quarter == 1:
            # 当前是当前年中报, 还需要 上一年年报 - 上一年年中报 + 当前年中报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q2_report = self.get_report_item(int("%d0630" % (year-1)), code)
            if len(year_report) == 0 or len(q2_report) == 0:
                # 上市不足一年
                if len(year_report) == 0:
                    self.logger.debug("report_quarter == 1, code:%s, year_report:%s is None" % (code, "%d1231" % (year-1)))
                else:
                    self.logger.debug("report_quarter == 1, code:%s, q2_report:%s is None" % (code, "%d0630" % (year-1)))
                return (price * cur_item['tcs'])/ cur_item['npbo']
            current_eps = (year_report['npbo'] - q2_report['npbo'] + cur_item['npbo']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
    
        if report_quarter == 0:
            # 当前是一季报, 还需要 上一年年报 - 上一年一季报 + 当前的一季报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q1_report = self.get_report_item(int("%d0331" % (year-1)), code)
            if len(year_report) == 0 or len(q1_report) == 0:
                # 上市不足一年
                if len(year_report) == 0:
                    self.logger.debug("report_quarter == 0, code:{}, year_report:{} is None".format(code, "%d1231" % (year-1)))
                else:
                    self.logger.debug("report_quarter == 0, code:{}, q1_report:{} is None".format(code, "%d0331" % (year-1)))
                return (price * cur_item['tcs'])/ cur_item['npbo']
            current_eps = (year_report['npbo'] - q1_report['npbo'] + cur_item['npbo']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
        self.logger.error("unexpected pe for code:%s, price:{}, date:{}".format(code, price, cur_item['date']))
        return 0.0

    cpdef dict get_actual_report_item(self, int mdate, str code, int timeToMarket):
        """
        根据当前的实际日期获取最新财报信息
        :param mdate:
        :param code:
        :return:
        """
        cdef dict item
        cdef int report_date
        global PRE_CUR_CODE, PRE_CUR_REPORT_DATE, PRE_CUR_ITEM
        report_date = report_date_with(mdate)

        if code == PRE_CUR_CODE and report_date == PRE_CUR_REPORT_DATE: return PRE_CUR_ITEM

        PRE_CUR_CODE = code
        PRE_CUR_REPORT_DATE = report_date
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于标准财报的披露时间，否则取用前一个财报信息
        if len(item) > 0 and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM
        elif len(item) == 0 and timeToMarket > report_date:
            self.logger.debug("{} timeToMarket {}, report_date:%{}".format(code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM
            
        self.logger.debug("{} has not publish report for normal months from {}, report_date:{}".format(code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于前一个财报披露时间
        if len(item) > 0 and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM
        elif len(item) == 0 and timeToMarket > report_date:
            self.logger.debug("{} timeToMarket {}, report_date:%{}".format(code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM

        self.logger.debug("{} has not publish report for 3 months from {}, report_date:{}".format(code, mdate, report_date))
        #只有这些垃圾股需要继续求取后面的日期，这些股票，不要也罢。
        #000048, 000939, 000995, 002260, 002604, 002680, 300028, 300104, 300216, 600074, 600610
        PRE_CUR_ITEM = dict()
        return PRE_CUR_ITEM

    def get_stock_pledge_info(self, code = None, mdate = None, dformat = '%Y%m%d'):
        if mdate is None: mdate = (datetime.now() - timedelta(days = 7)).strftime(dformat)
        if int(mdate) < 20180304: return None
        if datetime.strptime(mdate, dformat).weekday() == calendar.SUNDAY:
            cfrom_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
            cto_ = get_pre_date(mdate, target_day = calendar.SATURDAY, dformat = dformat)
        else:
            if datetime.strptime(mdate, dformat).weekday() == calendar.SATURDAY:
                cfrom_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
                cto_ = mdate
            else:
                cfrom_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
                cto_ = get_next_date(mdate, target_day = calendar.SATURDAY, dformat = dformat)
        filename = "%s_%s.xls" % (cfrom_, cto_)
        filepath = os.path.join(self.pledge_file_dir, filename)
        try:
            wb = xlrd.open_workbook(filepath, encoding_override="cp1252")
            name_list = ['nouse','date', 'code', 'name', 'counts', 'unlimited_quantity', 'limited_quantity', 'total_capital_share', 'pledge_rate']
            df = pd.read_excel(wb, sheet_name = 'Sheet1', engine = 'xlrd', header = 0, names = name_list, skiprows = [1,2])
            df['code'] = df['code'].map(lambda x: str(x).zfill(6))
            df = df[['date', 'code', 'pledge_rate']]
            df = df.reset_index(drop = True)
            if code is None:
                return df
            else:
                return df.loc[df.code == code].reset_index(drop = True)
        except Exception as e:
            self.logger.info(e)
            return None

    def get_stock_valuation(self, code, mdate):
        cdef object stock_obj = CStock(code)
        return stock_obj.get_val_data(mdate)

    cdef str get_r_financial_name(self, str mdate):
        #cdates = cdate.split('-')
        #return "%s_%s_%s.csv" % ("rval", cdates[0], (int(cdates[1])-1)//3 + 1)
        return "{}.csv".format(mdate)

    cdef object get_r_financial_data(self, str mdate):
        cdef str file_name = self.get_r_financial_name(mdate)
        cdef object file_path = Path(self.rvaluation_dir) / file_name
        if file_path.exists():
            use_cols = ['code', 'date', 'pe', 'pb', 'ttm', 'dr', 'ccs', 'tcs', 'ccs_mv', 'tcs_mv']
            dtype_dict = {'code':str, 'date': str, 'pe': float, 'pb': float, 'ttm': float, 'dr': float, 'ccs': float, 'tcs': float, 'ccs_mv': float, 'tcs_mv': float}
            return pd.read_csv(file_path, header = 0, encoding = "utf8", usecols = use_cols, dtype = dtype_dict)
        return DataFrame()

    cdef object index_val(self, object df, str dtype = 'pe'):
        cdef float total_mv = df['tcs_mv'].sum()
        cdef float total_profit = 0
        cdef object row
        for _, row in df.iterrows():
            total_profit += row['tcs_mv'] / row[dtype] if 0 != row[dtype] else 0 
        return total_mv / total_profit if total_profit != 0 else 0

    cdef object index_dr(self, object df):
        cdef float total_mv = df['tcs_mv'].sum()
        cdef float total_divide = df['dr'].dot(df['tcs_mv'])
        return total_divide / total_mv

    cpdef set_index_valuation(self, str code, str mdate):
        cdef dict data
        cdef object df, ndf
        cdef float pe, pb, ttm, roe, dr
        cdef object index_obj = CIndex(code)
        cdef object code_data = index_obj.get_components_data(mdate)
        if code_data is None or code_data.empty:
            self.logger.debug("get code:{}, mdate:{} empty.".format(code, mdate))
            return False
        code_list = code_data['code'].tolist()
        df = self.get_stocks_info(mdate, code_list)
        if df.empty: return False
        pe = self.index_val(df, 'pe')
        pb = self.index_val(df, 'pb')
        ttm = self.index_val(df, 'ttm')
        roe = pb / ttm if ttm != 0.0 else 0.0
        dr = self.index_dr(df)
        data = {'code':[code], 'date':[mdate], 'pe':[pe], 'pb':[pb], 'ttm':[ttm], 'roe':[roe], 'dr':[dr]}
        ndf = DataFrame.from_dict(data)
        return index_obj.set_val_data(ndf, mdate)

    cdef object get_stocks_info(self, str mdate, list code_list):
        cdef object df
        cdef object all_df = self.get_r_financial_data(mdate)
        if all_df.empty: return all_df
        df = all_df.loc[all_df.code.isin(code_list)]
        df = df.reset_index(drop = True)
        return df
