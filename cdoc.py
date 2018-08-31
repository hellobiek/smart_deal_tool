#-*- coding: utf-8 -*-
import os
import datetime
import traceback
from datetime import datetime
from markdown_writer import MarkdownWriter
from markdown_table import MarkdownTable
class CDoc:
    def __init__(self, fpath_dir):
        self.sdir = fpath_dir

    def generate(self, stock_info, industry_info, index_info):
        file_name = os.path.join(self.sdir, "%s-StockReView.md" % datetime.now().strftime('%Y-%m-%d'))
        f = open(file_name, "w+")

        md = MarkdownWriter()
        # add title
        md.addTitle()
        # header
        md.addHeader("股票复盘", 1)

        # 指数行情
        index_info = index_info[['name', 'open', 'high', 'close', 'low', 'volume', 'amount']]
        md.addHeader("指数行情", 2)
        t_index = MarkdownTable(headers = ["名称", "价格", "涨幅(百分比)", "成交量", "成交额(亿)"])
        for index in range(len(index_info)):
            data_list = index_info.loc[index].tolist()
            data_list = [data_list[0], round(data_list[3], 2), round(100 * (data_list[3] -  data_list[1]) / data_list[1] , 2), int(data_list[5]/100), round(data_list[6] / 100000000, 2)]
            data_list = [str(i) for i in data_list]
            t_index.addRow(data_list)
        md.addTable(t_index)

        # 行业资金
        industry_info = industry_info[['name', 'close', 'open', 'volume', 'amount']]
        total_amount = industry_info.amount.sum() / 100000000
        industry_info = industry_info[0:15]
        industry_info.amount = industry_info.amount / 100000000
        industry_info.volume = industry_info.volume / 100
        pchange = 100 * (industry_info.close - industry_info.open) / industry_info.open
        industry_info['pchange'] = pchange
        industry_info = industry_info[['name', 'close', 'pchange', 'volume', 'amount']]

        md.addHeader("行业资金", 2)
        t_industry = MarkdownTable(headers = ["名称", "价格", "涨幅", "占比(百分比)", "成交量", "成交额(亿)"])
        for index in range(len(industry_info)):
            data_list = industry_info.loc[index].tolist()
            data_list = [data_list[0], round(data_list[1], 2), round(data_list[2], 2), round(100 * data_list[4]/total_amount, 2), int(data_list[3]), round(data_list[4], 2)]
            data_list = [str(i) for i in data_list]
            t_industry.addRow(data_list)
        md.addTable(t_industry)

        # 股票数据
        stock_info = stock_info[['code', 'name', 'trade', 'changepercent', 'turnoverratio', 'volume', 'amount']]
        stock_info.amount = stock_info.amount / 100000000
        stock_info.volume = stock_info.volume / 100

        stock_info = stock_info.sort_values(by = 'amount', ascending= False)
        stock_info_amount = stock_info[0:100]
        stock_info_amount  = stock_info_amount.reset_index(drop = True)
        md.addHeader("股票数据(成交额)", 2)
        t_industry_amount = MarkdownTable(headers = ["代码", "名称", "价格(元)", "涨跌", "换手率(%)", "成交量", "成交额(亿)"])
        for index in range(len(stock_info_amount)):
            data_list = stock_info_amount.loc[index].tolist()
            data_list = [str(data_list[0]).zfill(6), data_list[1], round(data_list[2], 2), round(data_list[3], 2), round(data_list[4], 2), int(data_list[5]), round(data_list[6] , 2)]
            data_list = [str(i) for i in data_list]
            t_industry_amount.addRow(data_list)
        md.addTable(t_industry_amount)

        stock_info = stock_info.sort_values(by = 'turnoverratio', ascending= False)
        stock_info = stock_info.reset_index(drop = True)
        stock_info_turnover = stock_info[0:100]
        stock_info_turnover = stock_info_turnover.reset_index(drop = True)
        md.addHeader("股票数据(换手率)", 2)
        t_industry_turnover = MarkdownTable(headers = ["代码", "名称", "价格(元)", "涨跌", "换手率(%)", "成交量", "成交额(亿)"])
        for index in range(len(stock_info_turnover)):
            data_list = stock_info_turnover.loc[index].tolist()
            data_list = [str(data_list[0]).zfill(6), data_list[1], round(data_list[2], 2), round(data_list[3], 2), round(data_list[4], 2), int(data_list[5]), round(data_list[6], 2)]
            data_list = [str(i) for i in data_list]
            t_industry_turnover.addRow(data_list)
        md.addTable(t_industry_turnover)

        md.addHeader("情绪变化:", 2)
        md.addImage("emotion.png", imageTitle = "今日情绪")

        md.addHeader("涨跌统计:", 2)
        md.addImage("static.png", imageTitle = "涨跌统计")

        f.write(md.getStream())
        f.close()
