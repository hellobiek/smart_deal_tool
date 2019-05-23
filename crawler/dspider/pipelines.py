# -*- coding: utf-8 -*-
# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import xlrd
import zipfile
import tempfile
import const as ct
import pandas as pd
import dspider.poster as poster
from dspider import items
from scrapy import Request
from scrapy.exceptions import DropItem
from tempfile import TemporaryDirectory
from scrapy.pipelines.files import FilesPipeline

post_router = {
    items.MyDownloadItem:poster.MyDownloadItemPoster,
    items.PlateValuationItem:poster.PlateValuationPoster,
    items.HkexTradeTopTenItem:poster.HkexTradeTopTenItemPoster,
    items.HkexTradeOverviewItem:poster.HkexTradeOverviewPoster,
    items.SPledgeSituationItem:poster.SPledgeSituationItemPoster,
    items.InvestorSituationItem:poster.InvestorSituationItemPoster,
    items.MonthInvestorSituationItem:poster.MonthInvestorSituationItemPoster,
    items.ChinaSecurityIndustryValuationItem:poster.ChinaSecurityIndustryValuationPoster,
}

class DspiderPipeline(object):
    def process_item(self, item, spider):
        obj = post_router[item.__class__](item)
        if obj.check():
            obj.store()
        return item

class ChinaSecurityIndustryValuationHandlePipeline(object):
    def process_item(self, item, spider):
        fnames = item['file_name']
        fdir = ct.CHINA_SECURITY_INDUSTRY_VALUATION_PATH
        for fname in fnames:
            with zipfile.ZipFile(os.path.join(fdir, fname)) as f:
                with TemporaryDirectory() as dirpath:
                    for mfile in f.namelist():
                        f.extract(mfile, dirpath)
                        i_df, s_df = self.create_dataframe(dirpath, mfile)
                        self.store_df(s_df, ct.CHINA_SECURITY_INDUSTRY_VALUATION_STOCK_PATH)
                        self.store_df(i_df, ct.CHINA_SECURITY_INDUSTRY_VALUATION_INDUSTRY_PATH)
        return item

    def create_dataframe(self, fdir, fname):
        fpath = os.path.join(fdir, fname)
        cdate = "%s-%s-%s" % (fname[3:7], fname[7:9], fname[9:11])
        try:
            wb = xlrd.open_workbook(fpath, encoding_override="cp1252")
            stock_df = self.get_stock_df(wb, cdate)
            industry_df = self.get_industry_df(wb, cdate)
        except Exception as e:
            return pd.DataFrame(), pd.DataFrame()
        return industry_df, stock_df

    def store_df(self, df, fdir):
        if df.empty: return
        cdate = set(df.date.tolist()).pop()
        fname = '%s.csv' % cdate
        fpath = os.path.join(fdir, fname)
        df.to_csv(fpath)

    def store(self, item):
        obj = post_router[item.__class__](item)
        if obj.check(): obj.store()
       
    def get_stock_df(self, wb, cdate):
        name_list = ['code', 'name', 'pind_code', 'pind_name', 'sind_code', 'sind_name', 'tind_code', 'tind_name', 'find_code', 'find_name', 'pe', 'ttm', 'pb', 'dividend']
        df = pd.read_excel(wb, sheet_name = '个股数据', engine = 'xlrd', header = 0, names = name_list)
        if df.empty: return df
        df['date'] = cdate
        df['code'] = df['code'].map(lambda x : str(x).zfill(6))
        df['pind_code'] = df['pind_code'].map(lambda x : str(x).zfill(2))
        df['sind_code'] = df['sind_code'].map(lambda x : str(x).zfill(4))
        df['tind_code'] = df['tind_code'].map(lambda x : str(x).zfill(6))
        df['find_code'] = df['find_code'].map(lambda x : str(x).zfill(8))
        for col in ['pe', 'ttm', 'pb', 'dividend']:
            df[col] = df[col].apply(lambda x: float(x) if x != '-' else 0.0)
        return df

    def get_industry_df(self, wb, cdate):
        item_list = list()
        pb_sheet = wb.sheet_by_name('中证行业市净率')
        static_pe_sheet = wb.sheet_by_name('中证行业静态市盈率')
        rolling_pe_sheet = wb.sheet_by_name('中证行业滚动市盈率')
        dividend_yield_sheet = wb.sheet_by_name('中证行业股息率')
        for row in range(1, static_pe_sheet.nrows):
            item = items.ChinaSecurityIndustryValuationItem()
            item['date'] = cdate
            item['code'] = static_pe_sheet.cell(row, 0).value
            item['name'] = static_pe_sheet.cell(row, 1).value
            item['pe'] = item.convert(static_pe_sheet.cell(row, 2).value)
            item['ttm'] = item.convert(rolling_pe_sheet.cell(row, 2).value)
            item['pb'] = item.convert(pb_sheet.cell(row, 2).value)
            item['dividend'] = item.convert(dividend_yield_sheet.cell(row, 2).value)
            if not item.empty(): item_list.append(item)
        df = pd.DataFrame(item_list)
        return df

class PlateValuationHandlePipeline(object):
    def process_item(self, item, spider):
        fnames = item['file_name']
        fdir = ct.PLATE_VALUATION_PATH
        for fname in fnames:
            with zipfile.ZipFile(os.path.join(fdir, fname)) as f:
                with TemporaryDirectory() as dirpath:
                    for mfile in f.namelist():
                        f.extract(mfile, dirpath)
                        item_list = self.create_item(dirpath, mfile)
                        for mitem in item_list: self.store(mitem)
        return item
       
    def store(self, item):
        obj = post_router[item.__class__](item)
        if obj.check(): obj.store()
        
    def create_item(self, fdir, fname):
        fpath = os.path.join(fdir, fname)
        cdate = "%s-%s-%s" % (fname[2:6], fname[6:8], fname[8:10])
        try:
            wb = xlrd.open_workbook(fpath, encoding_override="cp1252")
        except Exception as e:
            return list()
        static_pe_sheet = wb.sheet_by_name('板块静态市盈率')
        rolling_pe_sheet = wb.sheet_by_name('板块滚动市盈率')
        pb_sheet = wb.sheet_by_name('板块市净率')
        dividend_yield_sheet = wb.sheet_by_name('板块股息率')
        item_list = list()
        for row in range(1, static_pe_sheet.nrows):
            item = items.PlateValuationItem()
            item['date'] = cdate
            name = static_pe_sheet.cell(row, 0).value
            item['name'] = name
            item['code'] = ct.PLATE_DICT[name]
            item['pe'] = item.convert(static_pe_sheet.cell(row, 1).value)
            item['ttm'] = item.convert(rolling_pe_sheet.cell(row, 1).value)
            item['pb'] = item.convert(pb_sheet.cell(row, 1).value)
            item['dividend'] = item.convert(dividend_yield_sheet.cell(row, 1).value)
            item_list.append(item)
        return item_list

class PlateValuationDownloadPipeline(FilesPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def file_path(self, request, response = None, info=None):
        return request.meta['item']['file_name'].strip('";')

    def get_media_requests(self, item, info):
        # FilesPepeline 根据file_urls指定的url进行爬取，该方法为每个url生成一个Request后 Return
        for file_url in item['file_urls']:
            yield Request(file_url, meta={'item': item})

    def item_completed(self, results, item, info):
        file_urls = [x['path'] for ok, x in results if ok]
        if not file_urls: raise DropItem("Item contains no files")
        item['file_name'] = file_urls
        return item

class SPledgePipline(FilesPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_media_requests(self, item, info):
        # FilesPepeline 根据file_urls指定的url进行爬取，该方法为每个url生成一个Request后 Return
        for file_url in item['file_urls']:
            yield Request(file_url, meta={'item': item})

    def item_completed(self, results, item, info):
        file_urls = [x['path'] for ok, x in results if ok]
        if not file_urls: raise DropItem("Item contains no files")
        item['file_name'] = file_urls
        return item

    def file_path(self, request, response = None, info=None):
        return request.meta['item']['file_name'].replace('gpzyhgmx_', '')
