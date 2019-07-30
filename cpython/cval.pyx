# -*- coding: utf-8 -*-
# cython: language_level=3, boundscheck=False, nonecheck=False, infer_types=True
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
from datetime import datetime
from base.clog import getLogger
from cstock_info import CStockInfo
from datamanager.cbonus import CBonus
from datamanager.creport import CReport
from base.cdate import quarter, report_date_with, int_to_datetime, prev_report_date_with, get_pre_date, get_next_date
DATA_COLUMS = ['date', 'code', 'bps', 'eps', 'np', 'ccs', 'tcs', 'roa', 'npm', 'fa', 'dar', 'gpr', 'publish']
DTYPE_DICT = {'date': int,
              'code':str, 
              'bps':float, 
              'eps': float, 
              'np': float, 
              'ccs': float, 
              'tcs': float, 
              'roa': float,
              'fa': float, #fixed assets
              'npm': float, #net profit margin
              'gpr': float, #gross profit ratio
              'dar': float, #debt asset ratio
              'cfpsfo': float, #cash flow per share from operations
              'micc': float, #main income cash content
              'crr': float, #cash recovery rate of all assets
              'ncf': float, #net cash flow to net profit ratio
              'revenue': float, #revenue
              'igr': float, #increase growth rate
              'ngr': float, #net growth rate
              'publish': int}

cdef str PRE_CUR_CODE = '', PRE_YEAR_CODE = ''
cdef dict PRE_YEAR_ITEM = dict(), PRE_CUR_ITEM = dict()
cdef int PRE_YEAR_REPORT_DATE = 0, PRE_CUR_REPORT_DATE = 0

cdef class CValuation(object):
    cdef public str report_data_path
    cdef public np.ndarray valuation_data
    cdef public object logger, bonus_client, report_client, stock_info_client
    def __init__(self, str valution_path = ct.VALUATION_PATH):
        self.logger = getLogger(__name__)
        self.bonus_client = CBonus()
        self.report_client = CReport()
        self.stock_info_client = CStockInfo()
        self.report_data_path = valution_path
        self.valuation_data = self.get_reports_data()

    cdef object get_reports_data(self):
        cdef object df
        if not Path(self.report_data_path).exists(): return None 
        df = pd.read_csv(self.report_data_path, header = 0, encoding = "utf8", usecols = DATA_COLUMS, dtype = DTYPE_DICT)
        df = df.drop_duplicates()
        df = df.reset_index(drop = True)
        return df.to_records(index = False)

    def convert(self):
        #date, code, 1.基本每股收益(earnings per share)、2.扣非每股收益(non-earnings per share)、
        #4.每股净资产(book value per share)、6.净资产回报率(roa)、72.所有者权益（或股东权益）合计(net assert)、
        #96.归属于母公司所有者的净利润(net profit)、238.总股本(total capital share)、239.已上市流通A股(circulating capital share)、
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
        #243.第一大股东的持股数量、248.QFII机构数、249.QFII持股量、250.券商机构数、251.券商持股量、
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
        #财报披露时间(publish)
        mcols = ['date','code',
                 'eps', #earnings per share
                 'neps', #non-earnings per share
                 'bps', #book value per share
                 'roa',
                 'na', #net assert
                 'np', #net profit
                 'tcs', #(total capital share
                 'ccs', #circulating capital share
                 'mf', #money funds
                 'br', #bill receivable
                 'ar', #accounts receivable
                 'prepayments', #prepayments
                 'or', #other receivables
                 'rfrc', #receivables from related companies
                 'rtr', #receivables turnover ratio
                 'dso', #days sales outstanding
                 'inventory',
                 'ta', #total assets
                 'stb', #short term borrowing
                 'bp', #bills payable
                 'ap', #accounts payable
                 'aria', #accounts received in advance
                 'pp', #payroll payable
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
                 'dar', #debt asset ratio
                 'iar', #inventory asset ratio
                 'cip', #construction in process
                 'de', #development expenditure
                 'revenue',
                 'opr', #operating profit ratio
                 'goodwill',
                 'holders',
                 'largest_holding', #第一大股东的持股数量
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
                 'igr', #increase growth rate
                 'ngr', #net growth rate
                 'pgr', #profit growth rate
                 'npr', #non-net profit rate
                 'ca', #current assets
                 'fa', #fixed assets
                 'npm', #net profit margin
                 'gpr', #gross profit ratio
                 'cfps', #cash flow per share
                 'micc', #main income cash content
                 'crr', #cash recovery rate of all assets
                 'ncf', #net cash flow to net profit ratio
                 'wcr', #working capital ratio
                 'qr', #quick ratio
                 'cr', #currency ratio
                 'cfpsfo', #cash flow per share from operations
                 'publish']
        date_list = self.report_client.get_all_report_list()
        is_first = True
        prefix = "col%s"
        for mdate in date_list:
            report_list = list()
            report_df = self.report_client.get_report_data(mdate)
            for idx, row in report_df.iterrows():
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
                            row[prefix%248], row[prefix%249], row[prefix%250], row[prefix%251], row[prefix%252], 
                            row[prefix%253], row[prefix%254], row[prefix%255], row[prefix%256], row[prefix%257], 
                            row[prefix%258], row[prefix%259], row[prefix%260], row[prefix%261], row[prefix%262], 
                            row[prefix%263], row[prefix%183], row[prefix%184], row[prefix%189], row[prefix%191],
                            row[prefix%21], row[prefix%27], row[prefix%199], row[prefix%202], row[prefix%225],
                            row[prefix%220], row[prefix%229], row[prefix%228], row[prefix%159], row[prefix%160],
                            row[prefix%161], row[prefix%219], pdate])
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
                self.logger.debug("get code:%s, tdate:%s failed. ccs:%s, tcs:%s" % (code, tdate, ccs, tcs))
                ccs, tcs = self.bonus_client.get_css_tcs(code, tdate)
                if ccs == 0 or tcs == 0:
                    self.logger.error("unexpected css tcs code:%s, tdate:%s for item is not None" % (code, tdate))
                    #sys.exit(0)
        else:
            ccs, tcs = self.bonus_client.get_css_tcs(code, tdate)
            if ccs == 0 or tcs == 0:
                self.logger.error("unexpected css tcs code:%s, tdate:%s for item is None" % (code, tdate))
                #sys.exit(0)
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
                        self.logger.error("code:%s, tdate:%s, year report publish date:%s, cur report publish date:%s" % (code, tdate, year_item['publish'], cur_item['publish']))
                        return tdate, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                        #sys.exit(0)
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

    def get_vertical_data(self, object df, list dtype_list, int mdate, str industry = '所有'):
        cdef str dtype
        cdef dict item
        cdef object dval
        def cfunc(str code, int time2Market):
            item = self.get_actual_report_item(mdate, code, time2Market)
            if 1 == len(dtype_list):
                return item[dtype_list[0]] if item else None
            else:
                return tuple([item[dtype] for dtype in dtype_list]) if item else tuple([None for dtype in dtype_list])
        vfunc = np.vectorize(cfunc)
        if len(dtype_list) == 1:
            df[dtype_list[0]] = vfunc(df['code'].values, df['timeToMarket'].values)
        else:
            for dtype, dval in zip(dtype_list, vfunc(df['code'].values, df['timeToMarket'].values)):
                df[dtype] = dval
        df = df.dropna(subset = dtype_list)
        df = df[(df[dtype_list] > 0).all(axis=1)]
        df = df.reset_index(drop = True)
        return df

    cpdef object get_horizontal_data(self, str code, list dtype_list):
        cdef object df = self.get_report_items(code)
        df = df[dtype_list]
        return df

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
            self.logger.debug("%s year report is empty for %s" % (report_date, code))
        else:
            if report_item["publish"] > mdate:
                #年报实际公布时间晚于当前时间
                report_date = int("%d1231" % (curday.year - 2))
                if timeToMarket > report_date:
                    PRE_YEAR_ITEM = dict()
                    return PRE_YEAR_ITEM
                report_item = self.get_report_item(report_date, code)
                if 0 == len(report_item):
                    self.logger.debug("get 2 years before report error., code:%s, date:%s" % (code, mdate))
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
                    self.logger.debug("report_quarter == 2, code:%s, year_report:%s is None" % (code, "%d1231" % (year-1)))
                else:
                    self.logger.debug("report_quarter == 2, code:%s, q3_report:%s is None" % (code, "%d0930" % (year-1)))
                return (price * cur_item['tcs'])/ cur_item['np']
    
            current_eps = (year_report['np'] - q3_report['np'] + cur_item['np']) / cur_item['tcs']
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
                return (price * cur_item['tcs'])/ cur_item['np']
            current_eps = (year_report['np'] - q2_report['np'] + cur_item['np']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
    
        if report_quarter == 0:
            # 当前是一季报, 还需要 上一年年报 - 上一年一季报 + 当前的一季报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q1_report = self.get_report_item(int("%d0331" % (year-1)), code)
            if len(year_report) == 0 or len(q1_report) == 0:
                # 上市不足一年
                if len(year_report) == 0:
                    self.logger.debug("report_quarter == 0, code:%s, year_report:%s is None" % (code, "%d1231" % (year-1)))
                else:
                    self.logger.debug("report_quarter == 0, code:%s, q1_report:%s is None" % (code, "%d0331" % (year-1)))
                return (price * cur_item['tcs'])/ cur_item['np']
            current_eps = (year_report['np'] - q1_report['np'] + cur_item['np']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
        self.logger.error("unexpected pe for code:%s, price:%s, date:%s" % (code, price, cur_item['date']))
        #sys.exit(0)
        return 0.0

    cpdef dict get_actual_report_item(self, int mdate, str code, int timeToMarket):
        """
        根据当前的实际日期获取最新财报信息
        :param mdate:
        :param code:
        :return:
        """
        cdef int report_date
        cdef dict item
        global PRE_CUR_CODE, PRE_CUR_REPORT_DATE, PRE_CUR_ITEM
        report_date = report_date_with(mdate)

        if code == PRE_CUR_CODE and report_date == PRE_CUR_REPORT_DATE: return PRE_CUR_ITEM

        PRE_CUR_CODE = code
        PRE_CUR_REPORT_DATE = report_date

        if timeToMarket > report_date:
            self.logger.debug("%s timeToMarket %s, report_date:%s" % (code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM

        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于标准财报的披露时间，否则取用前一个财报信息
        if len(item) > 0 and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM
        self.logger.debug("%s has not publish report for normal months from %s, report_date:%s" % (code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        if timeToMarket > report_date:
            self.logger.debug("%s timeToMarket %s, report_date:%s" % (code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于前一个财报披露时间
        if len(item) > 0  and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM

        self.logger.debug("%s has not publish report for 3 months from %s, report_date:%s" % (code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        if timeToMarket > report_date:
            self.logger.debug("%s timeToMarket %s, report_date:%s" % (code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于前一个财报披露时间
        if len(item) > 0 and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM
        self.logger.debug("%s has not publish report for 6 months from %sreport_date:%s" % (code, mdate, report_date))
        #000035 20041231日的年报，一直到20050815好才发布

        report_date = prev_report_date_with(report_date)
        if timeToMarket > report_date:
            self.logger.debug("%s timeToMarket %s, report_date:%s" % (code, timeToMarket, report_date))
            PRE_CUR_ITEM = dict()
            return PRE_CUR_ITEM
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于前一个财报披露时间
        if len(item) > 0 and item['publish'] <= mdate:
            PRE_CUR_ITEM = item
            return PRE_CUR_ITEM
        self.logger.debug("%s has not publish report for 9 months from %s, report_date:%s" % (code, mdate, report_date))
        PRE_CUR_ITEM = dict()
        return PRE_CUR_ITEM

    def get_stock_pledge_info(self, code = None, mdate = None, dformat = '%Y%m%d'):
        if mdate is None: mdate = datetime.now().strftime(dformat)
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
        filepath = os.path.join("/data/tdx/history/weeks/pledge", filename)
        try:
            wb = xlrd.open_workbook(filepath, encoding_override="cp1252")
            name_list = ['date', 'code', 'name', 'counts', 'unlimited_quantity', 'limited_quantity', 'total_capital_share', 'pledge_rate']
            df = pd.read_excel(wb, sheet_name = 'Sheet1', engine = 'xlrd', header = 0, names = name_list, skiprows = [1,2])
            df['code'] = df['code'].map(lambda x: str(x).zfill(6))
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
        return "%s.csv" % mdate

    cdef object get_r_financial_data(self, str mdate):
        cdef str file_name = self.get_r_financial_name(mdate)
        cdef object file_path = Path("/data/valuation/rstock") / file_name
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
        return total_mv / total_profit

    cdef object index_dr(self, object df):
        cdef float total_mv = df['tcs_mv'].sum()
        cdef float total_divide = df['dr'].dot(df['tcs_mv'])
        return total_divide / total_mv

    cpdef set_index_valuation(self, str code, str mdate):
        global ori_code_list
        cdef dict data
        cdef object df, ndf
        cdef float pe, pb, ttm, roe, dr
        cdef object index_obj = CIndex(code)
        cdef object code_data = index_obj.get_components_data(mdate)
        if code_data is None or code_data.empty:
            code_list = ori_code_list
        else:
            code_list = code_data['code'].tolist()
            ori_code_list = code_list
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
