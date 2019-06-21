# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import json
import xlrd
import calendar
import traceback
import const as ct
import pandas as pd
from cstock import CStock
from functools import partial
from datetime import datetime
from base.clog import getLogger
from cstock_info import CStockInfo
from datamanager.cbonus import CBonus
from datamanager.creport import CReport
from common import process_concurrent_run
from base.cdate import quarter, transfer_date_string_to_int, report_date_with, str_to_datetime, int_to_datetime, prev_report_date_with, get_pre_date, get_next_date
class CValuation(object):
    def __init__(self, valution_path = ct.VALUATION_PATH):
        self.logger = getLogger(__name__)
        self.bonus_client = CBonus()
        self.report_client = CReport()
        self.stock_info_client = CStockInfo()
        self.report_data_path = valution_path
        self.valuation_data = self.get_reports_data()

    def get_reports_data(self):
        #self.convert()
        df = pd.read_csv(self.report_data_path, header = 0, encoding = "utf8")
        df['code'] = df['code'].map(lambda x: str(x).zfill(6))
        return df

    def convert(self, mdate = None):
        #date, code, 1.基本每股收益(earnings per share)、2.扣非每股收益(non-earnings per share)、
        #4.每股净资产(book value per share)、6.净资产收益率(roe)、72.所有者权益（或股东权益）合计(net assert)、
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
        #189.营业利润增长率(profit growth rate)、191.扣非净利润同比(non-net profit rate)、财报披露时间(publish)
        mcols = ['date','code','eps','neps','bps',
                 'roe','na','np','tcs','ccs',
                 'mf','br','ar','prepayments','or',
                 'rfrc','rtr','dso','inventory','ta',
                 'stb','bp','ap','aria','pp',
                 'tadp','aip','cwa','ltl','tl',
                 'se','mc','ec','fc','it',
                 'dsoi','dar','iar','cip','de',
                 'revenue','opr','goodwill','holders','largest_holding',
                 'qfii_holders','qfii_holding','broker_holders','broker_holding','insurance_holders',
                 'insurance_holding','fund_holders','fund_holding','social_security_holders','social_security_holding',
                 'private_holders','private_holding','financial_company_holders', 'financial_company_holding','annuity_holders',
                 'annuity_holding','igr','ngr','pgr','npr','publish']
        date_list = self.report_client.get_all_report_list() if mdate is None else list(mdate)
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
                            row[prefix%263], row[prefix%183], row[prefix%184], row[prefix%189], row[prefix%191], pdate])
            result_df = pd.DataFrame(report_list, columns = mcols)
            result_df = result_df.sort_values(['code'], ascending = 1)
            result_df['code'] = result_df['code'].map(lambda x: str(x).zfill(6))
            if is_first is True:
                is_first = False
                result_df.to_csv(self.report_data_path, index=False, header=True, mode='w', encoding='utf8')
            else:
                result_df.to_csv(self.report_data_path, index=False, header=False, mode='a+', encoding='utf8')

    def get_css_tcs(self, tdate, code):
        item_valuation = self.get_actual_report_item(tdate, code)
        ccs = int(item_valuation["ccs"])    #流通股
        tcs = int(item_valuation["tcs"])    #总股本
        if ccs == 0 or tcs == 0: self.logger.error("code:%s, date:%s css:%s or tcs:%s is 0." % (code, tdate, css, tcs))
        return ccs, tcs

    def get_css_tcs_mv(self, close, ccs, tcs):
        ccs_mv = close * ccs
        tcs_mv = close * tcs
        return ccs_mv, tcs_mv

    def set_stock_valuation(self, base_df, mdate, code):
        def compute(tdate, close):
            tdate = int(tdate)
            if tdate > 20040101:
                pe_value = self.pe(tdate, code, close, time2market)
                ttm_value = self.ttm(tdate, code, close, time2market)
                pb_value = self.pb(tdate, code, close)
                roe_value = pb_value / ttm_value if ttm_value != 0.0 else 0.0
                ccs, tcs = self.get_css_tcs(tdate, code)
                ccs_mv, tcs_mv = self.get_css_tcs_mv(close, ccs, tcs)
                dividend_value = self.bonus_client.get_dividend_rate(tdate, code, close)
                return tdate, pe_value, ttm_value, pb_value, roe_value, dividend_value, ccs, tcs, ccs_mv, tcs_mv
            else:
                return tdate,0,0,0,0,0,0,0,0,0
        time2market = base_df.loc[base_df.code == code].timeToMarket.values[0]
        stock_obj = CStock(code)
        df, _ = stock_obj.read(mdate)
        tdates, pe_values, ttm_values, pb_values, roe_values, dividend_values, ccss, tcss, ccs_mvs, tcs_mvs = zip(*df.apply(lambda df: compute(df['date'], df['close']), axis = 1))
        data = {"date": list(tdates), "pe": list(pe_values), "ttm": list(ttm_values), "pb": list(pb_values), "roe": list(roe_values), 
                "dr": list(dividend_values), "ccs": list(ccss), "tcs": list(tcss), "ccs_mv": list(ccs_mvs), "tcs_mv": list(tcs_mvs)}
        vdf = pd.DataFrame(data, columns=["date", "pe", "ttm", "pb", "roe", "dr", "ccs", "tcs", "ccs_mv", "tcs_mv"])
        vdf['code'] = code
        return stock_obj.set_val_data(vdf, mdate)

    def set_financial_data(self, mdate = None):
        '''
        计算PE、PB、ROE、股息率、流通股本、总股本、流通市值、总市值
        1.基本每股收益、4.每股净资产、96.归属于母公司所有者的净利润、238.总股本、239.已上市流通A股
            总市值=当前股价×总股本
            PE=股价/每股收益
            PB=股价/每股净资产
            ROE=利润/每股净资产=PB/PE : 财报中已有静态的净资产收益率数据, 这里通过TTM计算一个大概的ROE作为参考
        '''
        try:
            base_df = self.stock_info_client.get_basics()
            code_list = base_df.code.tolist()
            fpath = "/tmp/succeed_list"
            with open(fpath) as f: succeed_list = f.read().strip().split()
            for code in base_df.code.tolist():
                if code not in succeed_list:
                    if self.set_stock_valuation(base_df, mdate, code):
                        succeed_list.append(code)
                        with open(fpath, 'a+') as f: f.write(code + '\n')
            #cfunc = partial(self.set_stock_valuation, base_df, mdate)
            #return process_concurrent_run(cfunc, code_list, num = 8)
        except Exception as e:
            self.logger.error(e)
            traceback.print_exc()

    def get_r_financial_name(self, mdate):
        cdates = cdate.split('-')
        return "%s_%s_%s.csv" % ("rval", cdates[0], (int(cdates[1])-1)//3 + 1)

    def get_stock_valuation(self, code, mdate):
        stock_obj = CStock(code)
        return stock_obj.get_val_data(mdate)

    def set_r_financial_data(self, mdate):
        try:
            file_name = self.get_r_financial_name(mdate)
            file_path = os.path.join("/data/valuation/rstock", file_name)
            base_df = self.stock_info_client.get_basics()
            code_list = base_df.code.tolist()
            all_df = pd.DataFrame()
            for code in base_df.code.tolist():
                df = self.get_stock_valuation(code, mdate)
                if not df.empey: all_df.append(df)
            if not os.path.exists(file_path):
                all_df.to_csv(file_path, index=False, header=True, mode='w', encoding='utf8')
            else:
                all_df.to_csv(file_path, index=False, header=False, mode='a+', encoding='utf8')
        except Exception as e:
            self.logger.error(e)
            traceback.print_exc()

    def get_r_financial_data(self, mdate):
        file_name = self.get_r_financial_name(mdate)
        file_path = os.path.join("/data/valuation/rstock", file_name)
        return pd.read_csv(file_path, header = 0, encoding = "utf8")

    def get_report_item(self, mdate, code):
        if mdate is None: return None
        df = self.valuation_data[(self.valuation_data["date"] == mdate) & (self.valuation_data["code"] == code)]
        return None if df.empty else list(df.to_dict('index').values())[0]

    def get_pe_report_item(self, mdate, code, time2market):
        curday = int_to_datetime(mdate)
        if curday.year < 1997:
            #1996年6月30号以前没有财报数据，静态市盈率需要年报数据，所以需要从1997年开始算起
            return None

        report_date = int("%d1231" % (curday.year - 1))
        if time2market < report_date:
            #上市时间早于标准年报更新时间
            report_item = self.get_report_item(report_date, code)
            if report_item is None:
                #获取不到年报
                self.logger.error("%s year report is empty for %s" % (report_date, code))
            else:
                if report_item["publish"] > mdate:
                    #年报实际公布时间晚于当前时间
                    self.logger.debug("%s year report actual report time is later than current date %s for %s" % (report_date, mdate, code))
                    report_date = int("%d1231" % (curday.year - 2))
                    report_item = self.get_report_item(report_date, code)
                    if report_item is None:
                        self.logger.error("get 2 years before report error., code:%s, date:%s" % (code, mdate))
        else:
            #上市时间晚于标准年报更新时间
            self.logger.error("%s release time is later than year report normal report time %s for %s" % (report_item, time2market, code))
            report_date = prev_report_date_with(report_date)
            report_item = self.get_report_item(report_date, code)
        return report_item

    def pb(self, mdate, code, price):
        """
        获取某只股票某个时段的PB值
        :param mdate:
        :param code:
        :param price:
        :return:
        """
        item = self.get_actual_report_item(mdate, code)
        if item is None or item['bps'] == 0: return 0.0
        if item['bps'] <= 0:
            self.logger.debug("code:%s, date:%s bps is less than zero" % (code, mdate))
        return price / item['bps']

    def pe(self, mdate, code, price, time2market):
        """
        获取某只股票某个时段的静态市盈率
        :param mdate: 日期
        :param code:  代码
        :param price: 价格
        :param time2market: 上市时间
        :return:
        """
        cur_item = self.get_actual_report_item(mdate, code)
        if cur_item is None or cur_item['tcs'] == 0:
            if mdate > 19970101:
                self.logger.error("code:%s, date:%s cur_item:%s is none or 0" % (code, mdate, cur_item))
            return 0.0
        year_item = self.get_pe_report_item(mdate, code, time2market)
        if year_item is None or year_item['tcs'] == 0:
            if mdate > 19970101:
                self.logger.error("code:%s, date:%s year_item:%s is none or 0" % (code, mdate, cur_item))
            return 0.0
        # 用年报的每股收益 * 因股本变动导致的稀释
        lyr_eps = year_item['eps'] * (year_item['tcs'] / cur_item['tcs'])
        return 0.0 if lyr_eps == 0 else price / lyr_eps

    def ttm(self, mdate, code, price, time2market):
        """
        获取指定日志的滚动市盈率(从2003年开始计算)
        :param date:
        :param code:
        :param price:
        :return:
        """
        cur_day = int_to_datetime(mdate)
        if cur_day.year <= 2002 or (cur_day.year == 2003 and cur_day.month < 4):
            # 2002年以前没有四季报，只算静态市盈率
            return self.pe(mdate, code, price, time2market)
   
        cur_item = self.get_actual_report_item(mdate, code)
        if cur_item is None or cur_item['tcs'] == 0.0:
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
            if year_report is None or q3_report is None: # 上市不足一年
                self.logger.error("report_quarter == 2, code:%s, date:%s item is None, cur_item['np']:%s" % (code, mdate, cur_item['np']))
                return price / (cur_item['np'] / cur_item['tcs'])
    
            current_eps = (year_report['np'] - q3_report['np'] + cur_item['np']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
    
        if report_quarter == 1:
            # 当前是当前年中报, 还需要 上一年年报 - 上一年年中报 + 当前年中报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q2_report = self.get_report_item(int("%d0630" % (year-1)), code)
            if year_report is None or q2_report is None:
                # 上市不足一年
                self.logger.error("report_quarter == 1, code:%s, date:%s item is None, cur_item['np']:%s" % (code, mdate, cur_item['np']))
                return price / (cur_item['np'] / cur_item['tcs'])
            current_eps = (year_report['np'] - q2_report['np'] + cur_item['np']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
    
        if report_quarter == 0:
            # 当前是一季报, 还需要 上一年年报 - 上一年一季报 + 当前的一季报
            year_report = self.get_report_item(int("%d1231" % (year-1)), code)
            q1_report = self.get_report_item(int("%d0331" % (year-1)), code)
            if year_report is None or q1_report is None:
                # 上市不足一年
                self.logger.error("report_quarter == 0, code:%s, date:%s item is None, cur_item['np']:%s" % (code, mdate, cur_item['np']))
                return price / (cur_item['np'] / cur_item['tcs'])
            current_eps = (year_report['np'] - q1_report['np'] + cur_item['np']) / cur_item['tcs']
            return 0.0 if current_eps == 0 else price / current_eps
        return 0.0

    def get_actual_report_item(self, mdate, code = None):
        """
        根据当前的实际日期获取最新财报信息
        :param mdate:
        :param code:
        :return:
        """
        report_date = report_date_with(mdate)
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于标准财报的披露时间，否则取用前一个财报信息
        if item is not None and item['publish'] <= mdate: return item
        self.logger.debug("%s has not publish report for normal months from %s, report_date:%s" % (code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        item = self.get_report_item(report_date, code)
        # 判断当前日期是否大于前一个财报披露时间，否则取用前两个财报信息
        if item is not None and item['publish'] <= mdate: return item
        self.logger.debug("%s has not publish report for 3 months from %s, report_date:%s" % (code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        item = self.get_report_item(report_date, code)

        # 判断当前日期是否大于前两个财报披露时间
        if item is not None and item['publish'] <= mdate: return item
        self.logger.debug("%s has not publish report for 6 months from %s, report_date:%s" % (code, mdate, report_date))

        report_date = prev_report_date_with(report_date)
        item = self.get_report_item(report_date, code)

        # 判断当前日期是否大于前三个财报披露时间
        if item is not None and item['publish'] <= mdate: return item
        self.logger.error("%s has not publish report for 6 months from %s, report_date:%s" % (code, mdate, report_date))

    def calculate(self, code_list = None):
        pass

    def get_stock_pledge_info(self, code = None, mdate = None, dformat = '%Y%m%d'):
        if mdate is None: mdate = datetime.now().strftime(dformat)
        if int(mdate) < 20180304: return None
        if datetime.strptime(mdate, dformat).weekday() == calendar.SUNDAY:
            from_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
            to_ = get_pre_date(mdate, target_day = calendar.SATURDAY, dformat = dformat)
        else:
            if datetime.strptime(mdate, dformat).weekday() == calendar.SATURDAY:
                from_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
                to_ = mdate
            else:
                from_ = get_pre_date(mdate, target_day = calendar.SUNDAY, dformat = dformat)
                to_ = get_next_date(mdate, target_day = calendar.SATURDAY, dformat = dformat)
        filename = "%s_%s.xls" % (from_, to_)
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
            self.logger.error(e)
            return None

if __name__ == '__main__':
    cvaluation = CValuation()
    #df = cvaluation.get_stock_pledge_info(mdate = '20180708')
    df = cvaluation.set_financial_data()
