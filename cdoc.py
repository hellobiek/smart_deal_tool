#-*- coding: utf-8 -*-
import os
import time
import json
import matplotlib
import const as ct
import numpy as np
from datetime import date
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from common import get_chinese_font
from mpl_finance import candlestick_ohlc
from markdown_table import MarkdownTable
from markdown_writer import MarkdownWriter
from visualization.marauder_map import MarauderMap
from algotrade.selecters.anti_market_up import AntiMarketUpSelecter
from algotrade.selecters.stronger_than_market import StrongerThanMarketSelecter
from algotrade.selecters.less_volume_in_high_profit import LowVolumeHighProfitSelecter
from algotrade.selecters.nei_chip_intensive import NeiChipIntensiveSelecter
from algotrade.selecters.bull_more_bear_less import BullMoreBearLessSelecter
from algotrade.selecters.game_kline_bigraise_and_large_volume import GameKLineBigraiseLargeVolumeSelecter
from algotrade.selecters.game_kline_bigraise_and_small_volume import GameKLineBigraiseSmallVolumeSelecter
class CDoc:
    COLORS = ['#F5DEB3', '#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082']
    def __init__(self):
        self.sdir = '/data/docs/blog/hellobiek.github.io/source/_posts'
        self.mmap_clinet = MarauderMap(ct.ALL_CODE_LIST)

    def emotion_plot(self, df, dir_name, file_name):
        fig = plt.figure()
        x = df.date.tolist()
        xn = range(len(x))
        y = df.score.tolist()
        plt.plot(xn, y)
        for xi, yi in zip(xn, y):
            plt.plot((xi,), (yi,), 'ro')
            plt.text(xi, yi, '%s' % yi)
        plt.scatter(xn, y, label='score', color='k', s=25, marker="o")
        plt.xticks(xn, x)
        plt.xlabel('时间', fontproperties = get_chinese_font())
        plt.ylabel('分数', fontproperties = get_chinese_font())
        plt.title('股市情绪', fontproperties = get_chinese_font())
        fig.autofmt_xdate()
        plt.savefig('%s/%s.png' % (dir_name, file_name), dpi=1000)

    def industry_plot(self, dir_name, industry_info):
        industry_info.amount = industry_info.amount / 10000000000
        total_amount = industry_info.amount.sum()
        amount_list = industry_info[0:10].amount.tolist()
        x = date.fromtimestamp(time.time())
        fig = plt.figure()
        base_line = 0 
        for i in range(len(amount_list)):
            label_name = "%s:%s" % (industry_info.loc[i]['name'], 100 * amount_list[i] / total_amount)
            plt.bar(x, amount_list[i], width = 0.1, color = self.COLORS[i], bottom = base_line, align = 'center', label = label_name)
            base_line += amount_list[i]
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.xlabel('x轴', fontproperties = get_chinese_font())
        plt.ylabel('y轴', fontproperties = get_chinese_font())
        plt.title('市值分布', fontproperties = get_chinese_font())
        fig.autofmt_xdate()
        plt.legend(loc = 'upper right', prop = get_chinese_font())
        plt.savefig('%s/industry.png' % dir_name, dpi=1000)

    def market_plot(self, sh_df, sz_df, x_dict, ycolumn, dir_name = '/code/figs'):
        y_dict = dict()
        y_dict['上海市场']  = sh_df[ycolumn].tolist()
        y_dict['深圳市场']  = sz_df[ycolumn].tolist()
        if ycolumn == 'turnover':
            y_dict['整体市场']  = ((sh_df[ycolumn] + sz_df[ycolumn])/2).round(2).tolist()
        else:
            y_dict['整体市场']  = (sh_df[ycolumn] + sz_df[ycolumn]).round(2).tolist()
        self.multi_plot(x_dict, y_dict, ycolumn, '%s trend' % ycolumn, dir_name, 'market_%s' % ycolumn)

    def multi_plot(self, x_dict, y_dict, ylabel, title, dir_name, filename):
        def _format_date(i, pos = None):
            if i < 0 or i > len(x) - 1: return ''
            return x[int(i)]

        xlabel  = list(x_dict.keys())[0]
        x       = list(x_dict.values())[0]
        xn      = range(len(x))

        fig = plt.figure()
        plt.title(title,   fontproperties = get_chinese_font())
        plt.xlabel(xlabel, fontproperties = get_chinese_font())
        plt.ylabel(ylabel, fontproperties = get_chinese_font())
        plt.gca().xaxis.set_major_locator(mticker.MultipleLocator(10))
        plt.gca().xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        i = 0
        for ylabel, y in y_dict.items():
            i += 1
            plt.plot(xn, y, label = ylabel)
            num = 0
            for xi, yi in zip(xn, y):
                if num % 7 == 0 or num == len(x) - 1:
                    plt.plot((xi,), (yi,), 'ro')
                    plt.text(xi, yi, '%s' % yi, fontsize = 7, rotation = 60)
                num += 1
            plt.scatter(xn, y, color = self.COLORS[i], s = 5, marker = "o")

        fig.autofmt_xdate()
        plt.legend(prop = get_chinese_font())
        plt.savefig('%s/%s.png' % (dir_name, filename), dpi=1000)

    def scatter_plot(x_data, y_data, x_label="", y_label="", title="", color = "r", yscale_log=False):
        # Create the plot object
        _, ax = plt.subplots()
        # Plot the data, set the size (s), color and transparency (alpha) of the points
        ax.scatter(x_data, y_data, s = 10, color = color, alpha = 0.75)
        if yscale_log == True:
            ax.set_yscale('log')
        # Label the axes and provide a title
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

    def plot_ohlc(self, df, ylabel, flabel, dir_name, filename):
        date_tickers = df.date.tolist()
        def _format_date(x, pos = None):
            if x < 0 or x > len(date_tickers) - 1: return ''
            return date_tickers[int(x)]

        fig, ax = plt.subplots(figsize = (16, 10))
        fig.subplots_adjust(bottom = 0.2)
        candlestick_ohlc(ax, df.values, width = 1.0, colorup = 'r', colordown = 'g')
        ax.xaxis.set_major_locator(mticker.MultipleLocator(3))
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_date))
        ax.set_ylabel(ylabel, fontproperties = get_chinese_font())
        ax.set_title(flabel, fontproperties = get_chinese_font())
        ax.yaxis.label.set_color("k")
        ax.grid(True, color = 'k', linestyle = '--')
        fig.autofmt_xdate()
        plt.savefig('%s/%s.png' % (dir_name, filename), dpi=1000)

    def plot_pie(self, df, column, title, xtuple, dir_name, filename, ctype = None):
        def xfunc(pct, allvals):
            absolute = int(pct / 100. * np.sum(allvals))
            return "{:.1f}%".format(pct)
        df = df[['name', 'code', column]]
        data = df[column].tolist()
        fig, ax = plt.subplots(figsize = (6, 4), subplot_kw = dict(aspect = "equal"))
        ingredients = (df.name + ':' + df.code).tolist()
        ax.set_title(title, fontproperties = get_chinese_font())
        if ctype is not None:
            wedges, texts, autotexts = ax.pie(data, radius = 1, labels = xtuple, autopct = lambda pct: xfunc(pct, data), textprops = dict(color = "w", fontproperties = get_chinese_font()))
            ax.legend(wedges, ingredients, title = 'name', loc = "lower left", bbox_to_anchor=(1, 0), prop = get_chinese_font(), fontsize = 'x-small')
            plt.setp(autotexts, size = 7)
            plt.setp(texts, size = 7, color = 'b')
        else:
            wedges, texts = ax.pie(data, radius = 1, labels = xtuple, textprops = dict(color = "w", fontproperties = get_chinese_font()))
            ax.legend(wedges, ingredients, title = 'name',  loc = "lower left", bbox_to_anchor=(1, 0), prop = get_chinese_font(), fontsize = 'x-small')
            plt.setp(texts, size = 8, color = 'b')
        fig.autofmt_xdate()
        plt.savefig('%s/%s.png' % (dir_name, filename), dpi = 1000)

    def static_plot(self, stock_info, limit_info,  dir_name, file_name):
        limit_up_list   = limit_info[(limit_info.pchange > 0) & (limit_info.prange != 0)].reset_index(drop = True).code.tolist()
        limit_down_list = limit_info[limit_info.pchange < 0].reset_index(drop = True).code.tolist()
        limit_list = limit_up_list + limit_down_list
        stock_info = stock_info[~stock_info.code.isin(limit_list)]
        changepercent_list = [9, 7, 5, 3, 1, 0, -1, -3, -5, -7, -9]
        num_list = list()
        name_list = list()
        num_list.append(len(limit_up_list))
        name_list.append("涨停")
        c_length = len(changepercent_list)
        for index in range(c_length):
            pchange = changepercent_list[index]
            if 0 == index:
                num_list.append(len(stock_info[stock_info.pchange > pchange]))
                name_list.append(">%s" % pchange)
            elif c_length - 1 == index:
                num_list.append(len(stock_info[stock_info.pchange < pchange]))
                name_list.append("<%s" % pchange)
            else:
                p_max_change = changepercent_list[index - 1]
                num_list.append(len(stock_info[(stock_info.pchange > pchange) & (stock_info.pchange < p_max_change)]))
                name_list.append("%s-%s" % (pchange, p_max_change))
        num_list.append(len(limit_down_list))
        name_list.append("跌停")
        fig = plt.figure()
        for i in range(len(num_list)):
            plt.bar(i + 1, num_list[i], color = self.COLORS[i % len(self.COLORS)], width = 0.3)
            plt.text(i + 1, 15 + num_list[i], num_list[i], ha = 'center', font_properties = get_chinese_font())
    
        plt.xlabel('x轴', fontproperties = get_chinese_font())
        plt.ylabel('y轴', fontproperties = get_chinese_font())
        plt.title('涨跌分布', fontproperties = get_chinese_font())
        plt.xticks(range(1, len(num_list) + 1), name_list, fontproperties = get_chinese_font())
        fig.autofmt_xdate()
        plt.savefig('%s/%s.png' % (dir_name, file_name), dpi=1000)

    def generate(self, cdate, sh_df, sz_df, sh_rzrq_df, sz_rzrq_df, av_df, limit_info, stock_info, industry_info, index_info, all_stock_info):
        image_dir = os.path.join(self.sdir, "%s-StockReView" % cdate)
        file_name = "%s.md" % image_dir
        if os.path.exists(file_name): return True
        os.makedirs(image_dir, exist_ok = True)

        md = MarkdownWriter()
        md.addTitle(cdate)
        md.addHeader("股票复盘", 1)
        # 资金面分析
        md.addHeader("资金面分析:", 2)
        x_dict = dict()
        x_dict['日期'] = sh_df.date.tolist()
        #上海和深圳的成交额分析
        md.addHeader("成交额分析:", 3)
        self.market_plot(sh_df, sz_df, x_dict, 'amount', dir_name = image_dir)
        md.addImage("market_amount.png", imageTitle = "成交额")
        #上海和深圳的流通市值分析
        md.addHeader("流通市值分析:", 3)
        self.market_plot(sh_df, sz_df, x_dict, 'negotiable_value', dir_name = image_dir)
        md.addImage("market_negotiable_value.png", imageTitle = "流通市值")
        #上海和深圳的换手率分析
        md.addHeader("市场换手率分析:", 3)
        self.market_plot(sh_df, sz_df, x_dict, 'turnover', dir_name = image_dir)
        md.addImage("market_turnover.png", imageTitle = "换手率")
        #上海和深圳的融资融券分析
        md.addHeader("融资融券分析:", 3)
        y_dict = dict()
        y_dict['日期'] = sh_rzrq_df.date.tolist()
        self.market_plot(sh_rzrq_df, sz_rzrq_df, y_dict, 'rzrqye', dir_name = image_dir)
        md.addImage("market_rzrqye.png", imageTitle = "融资融券")
        #平均股价走势
        md.addHeader("平均股价分析:", 3)
        self.plot_ohlc(av_df, '平均股价', '平均股价走势图', image_dir, 'average_price')
        md.addImage("average_price.png", imageTitle = "平均股价")
        #活点地图
        md.addHeader("活点地图分析:", 3)
        self.mmap_clinet.plot(cdate, image_dir, 'marauder_map')
        md.addImage("marauder_map.png", imageTitle = "活点地图")
        #涨停分析
        md.addHeader("涨停跌停分析:", 3)
        self.static_plot(stock_info, limit_info, dir_name = image_dir, file_name = 'pchange_static')
        md.addImage("pchange_static.png", imageTitle = "活点地图")

        #行业分析
        md.addHeader("行业分析:", 2)
        ##总成交额分析
        total_amount = industry_info['amount'].sum()
        df = industry_info.sort_values(by = 'amount', ascending= False)
        df = df[['name', 'code', 'amount']]
        df = df.head(min(9, len(df)))
        df.at[len(df)] = ['其他', '999999', total_amount - df['amount'].sum()]
        df['amount']   = df['amount'] / 1e8
        xtuple = tuple((df['name'] + ':' + df['amount'].astype('str') + '亿').tolist())
        md.addHeader("总成交额分析:", 3)
        self.plot_pie(df, 'amount', '每日成交额行业分布', xtuple, image_dir, 'industry_amount_distribution', ctype = 'func')
        md.addImage("industry_amount_distribution.png", imageTitle = "总成交额分析")

        ##总涨幅分析
        df = industry_info[industry_info['pchange'] > 0]
        if not df.empty:
            df = df[['name', 'code', 'pchange']]
            df = df.sort_values(by = 'pchange', ascending= False)
            df = df.head(min(10, len(df)))
            xtuple = tuple((df['name'] + ':' + df['pchange'].astype('str') + '%').tolist())
            md.addHeader("总涨幅分析:", 3)
            self.plot_pie(df, 'pchange', '每日涨幅行业分布', xtuple, image_dir, 'industry_price_increase_distribution')
            md.addImage("industry_price_increase_distribution.png", imageTitle = "总涨幅分析")

        ##金额增加额的行业分布
        df = industry_info[industry_info['money_change'] > 0]
        if not df.empty:
            df = df[['name', 'code', 'money_change']]
            df = df.sort_values(by = 'money_change', ascending= False)
            df = df.head(min(10, len(df)))
            xtuple = tuple((df['name'] + ':' + df['money_change'].astype('str') + '亿').tolist())
            md.addHeader("金额增加额的行业分布:", 3)
            self.plot_pie(df, 'money_change', '每日成交增加额行业分布', xtuple, image_dir, 'industry_money_increase_distribution')
            md.addImage("industry_money_increase_distribution.png", imageTitle = "金额增加额的行业分布")

        ##金额增加百分比的行业分布
        df = industry_info[industry_info['mchange'] > 0]
        if not df.empty:
            df = df[['name', 'code', 'mchange']]
            df = df.sort_values(by = 'mchange', ascending= False)
            df = df.head(min(10, len(df)))
            xtuple = tuple((df['name'] + ':' + df['mchange'].astype('str') + '%').tolist())
            md.addHeader("金额增加百分比的行业分布:", 3)
            self.plot_pie(df, 'mchange', '每日成交增加比例行业分布', xtuple, image_dir, 'industry_money_increase_percent_distribution')
            md.addImage("industry_money_increase_percent_distribution.png", imageTitle = "金额增加百分比的行业分布")

        ##总跌幅分析
        df = industry_info[industry_info['pchange'] < 0]
        if not df.empty:
            df = df[['name', 'code', 'pchange']]
            df = df.sort_values(by = 'pchange', ascending= True)
            df = df.head(min(10, len(df)))
            df['pchange'] = df['pchange'] * -1
            xtuple = tuple((df['name'] + '跌幅:' + df['pchange'].astype('str') + '%').tolist())
            md.addHeader("总跌幅分析:", 3)
            self.plot_pie(df, 'pchange', '每日涨幅行业分布', xtuple, image_dir, 'industry_price_decrease_distribution')
            md.addImage("industry_price_decrease_distribution.png", imageTitle = "总跌幅分析")

        ##金额减少额的行业分布
        df = industry_info[industry_info['money_change'] < 0]
        if not df.empty:
            df = df[['name', 'code', 'money_change']]
            df = df.sort_values(by = 'money_change', ascending= True)
            df = df.head(min(10, len(df)))
            df['money_change'] = df['money_change'] * -1
            xtuple = tuple((df['name'] + ':减少' + df['money_change'].astype('str') + '亿').tolist())
            md.addHeader("金额减少额的行业分布:", 3)
            self.plot_pie(df, 'money_change', '每日成交减少额行业分布', xtuple, image_dir, 'industry_money_decrease_distribution')
            md.addImage("industry_money_decrease_distribution.png", imageTitle = "金额减少额的行业分布")

        ##金额减少百分比的行业分布
        df = industry_info[industry_info['mchange'] < 0]
        if not df.empty:
            df = df[['name', 'code', 'mchange']]
            df = df.sort_values(by = 'mchange', ascending= False)
            df = df.head(min(10, len(df)))
            df['mchange'] = df['mchange'] * -1
            xtuple = tuple((df['name'] + ':减少' + df['mchange'].astype('str') + '%').tolist())
            md.addHeader("金额减少百分比的行业分布:", 3)
            self.plot_pie(df, 'mchange', '每日成交减少百分比行业分布', xtuple, image_dir, 'industry_money_decrease_percent_distribution')
            md.addImage("industry_money_decrease_percent_distribution.png", imageTitle = "金额减少百分比的行业分布")

        #指数行情
        index_info = index_info[['name', 'open', 'high', 'close', 'low', 'volume', 'amount']]
        md.addHeader("指数行情", 2)
        t_index = MarkdownTable(headers = ["名称", "价格", "涨幅(百分比)", "成交量", "成交额(亿)"])
        for index in range(len(index_info)):
            data_list = index_info.loc[index].tolist()
            data_list = [data_list[0], round(data_list[3], 2), round(100 * (data_list[3] -  data_list[1]) / data_list[1] , 2), int(data_list[5]/100), round(data_list[6] / 100000000, 2)]
            data_list = [str(i) for i in data_list]
            t_index.addRow(data_list)
        md.addTable(t_index)

        #选股指标
        md.addHeader("选股器选股", 2)
        t_selector = MarkdownTable(headers = ["方法", "股票列表"])

        stm = StrongerThanMarketSelecter()
        stm_code_list = stm.choose(all_stock_info, av_df)
        t_selector.addRow(['强于平均股价5%', json.dumps(stm_code_list)])

        amus = AntiMarketUpSelecter()
        amus_code_list = amus.choose(stock_info)
        t_selector.addRow(['逆势上涨', json.dumps(stm_code_list)])

        lvhps = LowVolumeHighProfitSelecter()
        lvhps_code_list = lvhps.choose(stock_info)
        t_selector.addRow(['高盈利低换手', json.dumps(lvhps_code_list)])

        gkblvs = GameKLineBigraiseLargeVolumeSelecter()
        gkblvs_code_list = gkblvs.choose(stock_info)
        t_selector.addRow(['博弈K线带量长阳', json.dumps(gkblvs_code_list)])

        gkbsvs = GameKLineBigraiseSmallVolumeSelecter()
        gkbsvs_code_list = gkbsvs.choose(stock_info)
        t_selector.addRow(['博弈K线无量长阳', json.dumps(gkbsvs_code_list)])

        ncis = NeiChipIntensiveSelecter()
        ncis_code_list = ncis.choose(stock_info)
        t_selector.addRow(['低位筹码密集', json.dumps(ncis_code_list)])

        bmbl = BullMoreBearLessSelecter()
        bmbl.choose(all_stock_info)

        md.addTable(t_selector)
        with open(file_name, "w+") as f:
            f.write(md.getStream())
