#-*- coding: utf-8 -*-
import gevent
from gevent import monkey
monkey.patch_all(thread = True)
from gevent.pool import Pool
import os
import time
import _pickle
import datetime
import matplotlib
import const as ct
import numpy as np
import pandas as pd
matplotlib.use('Agg')
from matplotlib import style
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.animation as animation
from cdoc import CDoc
from log import getLogger
from climit import CLimit
from cindex import CIndex
from cmysql import CMySQL
from functools import partial
from rstock import RIndexStock
from ccalendar import CCalendar
import matplotlib.pyplot as plt
from datetime import datetime, date
from datamanager.margin import Margin
from rindustry import RIndexIndustryInfo
from industry_info import IndustryInfo
from mpl_finance import candlestick_ohlc
from datamanager.emotion import Emotion
from datamanager.sexchange import StockExchange
from visualization.marauder_map import MarauderMap 
from algotrade.selecters.anti_market_up import AntiMarketUpSelecter
from algotrade.selecters.stronger_than_market import StrongerThanMarketSelecter
from algotrade.selecters.less_volume_in_high_profit import LessVolumeHighProfitSelecter
from algotrade.selecters.game_kline_bigraise_and_large_volume import GameKLineBigraiseLargeVolumeSelecter
from common import create_redis_obj, get_chinese_font, get_tushare_client, get_day_nday_ago

class CReivew:
    SSE  = 'SSE'
    SZSE = 'SZSE'
    COLORS = ['#F5DEB3', '#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082']
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo             = dbinfo
        self.logger             = getLogger(__name__)
        self.tu_client          = get_tushare_client()
        self.sdir               = '/data/docs/blog/hellobiek.github.io/source/_posts'
        self.doc                = CDoc(self.sdir)
        self.redis              = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client       = CMySQL(dbinfo, iredis = self.redis)
        self.margin_client      = Margin(dbinfo = dbinfo, redis_host = redis_host)
        self.rstock_client      = RIndexStock(dbinfo = dbinfo, redis_host = redis_host) 
        self.sh_market_client   = StockExchange(ct.SH_MARKET_SYMBOL)
        self.sz_market_client   = StockExchange(ct.SZ_MARKET_SYMBOL)
        self.emotion_client     = Emotion()
        self.mmap_clinet        = MarauderMap(ct.ALL_CODE_LIST)

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

    def get_industry_data(self, cdate):
        ri = RIndexIndustryInfo()
        df = ri.get_k_data(cdate)
        if df.empty: return df
        df = df.reset_index(drop = True)
        df = df.sort_values(by = 'amount', ascending= False)
        df['money_change'] = (df['amount'] - df['preamount'])/1e8
        industry_info = IndustryInfo.get()
        df = pd.merge(df, industry_info, how='left', on=['code'])
        return df

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

    def get_limitup_data(self, date):
        return CLimit(self.dbinfo).get_data(date)

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

    def get_index_data(self, _date):
        df = pd.DataFrame()
        for code, name in ct.TDX_INDEX_DICT.items():
            self.mysql_client.changedb(CIndex.get_dbname(code))
            data = self.mysql_client.get("select * from day where date=\"%s\";" % _date)
            data['name'] = name
            data['code'] = code
            df = df.append(data)
        self.mysql_client.changedb()
        return df

    def get_market_data(self, market, start_date, end_date):
        if market == ct.SH_MARKET_SYMBOL:
            df = self.sh_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '上海市场']
        else:
            df = self.sz_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '深圳市场']
        df                  = df.round(2)
        df                  = df.drop_duplicates()
        df                  = df.reset_index(drop = True)
        df                  = df.sort_values(by = 'date', ascending= True)
        df.negotiable_value = (df.negotiable_value / 2).astype(int)
        return df

    def market_plot(self, sh_df, sz_df, x_dict, ycolumn):
        y_dict = dict()
        y_dict['上海市场']  = sh_df[ycolumn].tolist()
        y_dict['深圳市场']  = sz_df[ycolumn].tolist()
        if ycolumn == 'turnover':
            y_dict['整体市场']  = ((sh_df[ycolumn] + sz_df[ycolumn])/2).round(2).tolist()
        else:
            y_dict['整体市场']  = (sh_df[ycolumn] + sz_df[ycolumn]).round(2).tolist()
        self.multi_plot(x_dict, y_dict, ycolumn, '%s trend' % ycolumn, '/code/figs', 'market_%s' % ycolumn)

    def get_rzrq_info(self, market, start_date, end_date):
        df = self.margin_client.get_k_data_in_range(start_date, end_date)
        if market == ct.SH_MARKET_SYMBOL:
            df = df.loc[df.code == 'SSE']
            df['code'] = '上海市场'
        else:
            df = df.loc[df.code == 'SZSE']
            df['code'] = '深圳市场'
        df           = df.round(2)
        df['rzye']   = df['rzye']/1e+8
        df['rzmre']  = df['rzmre']/1e+8
        df['rzche']  = df['rzche']/1e+8
        df['rqye']   = df['rqye']/1e+8
        df['rzrqye'] = df['rzrqye']/1e+8
        df = df.drop_duplicates()
        df = df.reset_index(drop = True)
        df = df.sort_values(by = 'date', ascending= True)
        return df

    def get_index_df(self, code, start_date, end_date):
        cindex_client = CIndex(code)
        df = cindex_client.get_k_data_in_range(start_date, end_date)
        df['time'] = df.index.tolist()
        df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return df

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
        df   = df[['name', 'code', column]]
        data = df[column].tolist()

        fig, ax     = plt.subplots(figsize = (6, 3), subplot_kw = dict(aspect = "equal"))
        ingredients = (df.name + ':' + df.code).tolist()
        fig.autofmt_xdate()
        ax.set_title(title, fontproperties = get_chinese_font())
        if ctype is not None:
            wedges, texts, autotexts = ax.pie(data, labels = xtuple, autopct = lambda pct: xfunc(pct, data), textprops = dict(color = "w", fontproperties = get_chinese_font()))
            ax.legend(wedges, ingredients, title = 'name', loc = "upper right", bbox_to_anchor=(1, 0, 1, 1), prop = get_chinese_font(), fontsize = 'x-small')
            plt.setp(autotexts, size = 6)
            plt.setp(texts, size = 6, color = 'b')
        else:
            wedges, texts = ax.pie(data, labels = xtuple, textprops = dict(color = "w", fontproperties = get_chinese_font()))
            ax.legend(wedges, ingredients, title = 'name',  loc = "upper right", bbox_to_anchor=(1, 0, 1, 1), prop = get_chinese_font(), fontsize = 'x-small')
            plt.setp(texts, size = 6, color = 'b')
        plt.savefig('%s/%s.png' % (dir_name, filename), dpi = 1000)

    def update(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        start_date = get_day_nday_ago(cdate, 100, dformat = "%Y-%m-%d")
        end_date   = cdate
        dir_name   = os.path.join(self.sdir, "%s-StockReView" % cdate)
        try:
            self.logger.info("create daily info")
            if not os.path.exists(dir_name):
                self.logger.info("market analysis")
                sh_df = self.get_market_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
                sz_df = self.get_market_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)
                sh_rzrq_df = self.get_rzrq_info(ct.SH_MARKET_SYMBOL, start_date, end_date)
                sz_rzrq_df = self.get_rzrq_info(ct.SZ_MARKET_SYMBOL, start_date, end_date)
                #平均股价的数据
                av_df = self.get_index_df('880003', start_date, end_date)

                #x_dict = dict()
                #x_dict['日期'] = sh_df.date.tolist()
                #self.market_plot(sh_df, sz_df, x_dict, 'amount')
                #self.market_plot(sh_df, sz_df, x_dict, 'negotiable_value')
                #self.market_plot(sh_df, sz_df, x_dict, 'number')
                #self.market_plot(sh_df, sz_df, x_dict, 'turnover')
                #self.market_plot(sh_rzrq_df, sz_rzrq_df, x_dict, 'rzrqye')
                #self.plot_ohlc(av_df, '平均股价', '平均股价走势图', '/code/figs', 'average_price')
                #self.mmap_clinet.plot(cdate, '/code/figs', 'marauder_map')

                ##limit up and down info
                #limit_info = self.get_limitup_data(cdate)
                today_stock_info = self.rstock_client.get_data(cdate)
                ##get volume > 0 stock list
                today_stock_info = today_stock_info[today_stock_info.volume > 0]
                today_stock_info = today_stock_info.reset_index(drop = True)
                ##static analysis
                #self.static_plot(today_stock_info, limit_info, dir_name = '/code/figs', file_name = 'pchange static')

                ##板块分析
                #industry_data = self.get_industry_data(cdate)

                ##总成交额分析
                #total_amount = industry_data['amount'].sum()
                #df = industry_data.sort_values(by = 'amount', ascending= False)
                #df = df[['name', 'code', 'amount']]
                #df = df.head(min(9, len(df)))
                #df.at[len(df)] = ['其他', '999999', total_amount - df['amount'].sum()]
                #df['amount']   = df['amount'] / 1e8
                #xtuple = tuple((df['name'] + ':' + df['amount'].astype('str') + '亿').tolist())
                #self.plot_pie(df, 'amount', '每日成交额行业分布', xtuple, '/code/figs', 'industry amount distribution', ctype = 'func')

                ##总涨幅分析
                #df = industry_data[industry_data['pchange'] > 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'pchange']]
                #    df = df.sort_values(by = 'pchange', ascending= False)
                #    df = df.head(min(10, len(df)))
                #    xtuple = tuple((df['name'] + ':' + df['pchange'].astype('str') + '%').tolist())
                #    self.plot_pie(df, 'pchange', '每日涨幅行业分布', xtuple, '/code/figs', 'industry price increase distribution')

                ##金额增加额的行业分布
                #df = industry_data[industry_data['money_change'] > 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'money_change']]
                #    df = df.sort_values(by = 'money_change', ascending= False)
                #    df = df.head(min(10, len(df)))
                #    xtuple = tuple((df['name'] + ':' + df['money_change'].astype('str') + '亿').tolist())
                #    self.plot_pie(df, 'money_change', '每日成交增加额行业分布', xtuple, '/code/figs', 'industry money increase distribution')

                ##金额增加百分比的行业分布
                #df = industry_data[industry_data['mchange'] > 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'mchange']]
                #    df = df.sort_values(by = 'mchange', ascending= False)
                #    df = df.head(min(10, len(df)))
                #    xtuple = tuple((df['name'] + ':' + df['mchange'].astype('str') + '%').tolist())
                #    self.plot_pie(df, 'mchange', '每日成交增加比例行业分布', xtuple, '/code/figs', 'industry money increase percent distribution')

                ##总跌幅分析
                #df = industry_data[industry_data['pchange'] < 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'pchange']]
                #    df = df.sort_values(by = 'pchange', ascending= True)
                #    df = df.head(min(10, len(df)))
                #    df['pchange'] = df['pchange'] * -1
                #    xtuple = tuple((df['name'] + '跌幅:' + df['pchange'].astype('str') + '%').tolist())
                #    self.plot_pie(df, 'pchange', '每日涨幅行业分布', xtuple, '/code/figs', 'industry price decrease distribution')

                ##金额减少额的行业分布
                #df = industry_data[industry_data['money_change'] < 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'money_change']]
                #    df = df.sort_values(by = 'money_change', ascending= True)
                #    df = df.head(min(10, len(df)))
                #    df['money_change'] = df['money_change'] * -1
                #    xtuple = tuple((df['name'] + ':减少' + df['money_change'].astype('str') + '亿').tolist())
                #    self.plot_pie(df, 'money_change', '每日成交减少额行业分布', xtuple, '/code/figs', 'industry money decrease distribution')

                ##金额减少百分比的行业分布
                #df = industry_data[industry_data['mchange'] < 0]
                #if not df.empty:
                #    df = df[['name', 'code', 'mchange']]
                #    df = df.sort_values(by = 'mchange', ascending= False)
                #    df = df.head(min(10, len(df)))
                #    df['mchange'] = df['mchange'] * -1
                #    xtuple = tuple((df['name'] + ':减少' + df['mchange'].astype('str') + '%').tolist())
                #    self.plot_pie(df, 'mchange', '每日成交减少百分比行业分布', xtuple, '/code/figs', 'industry money decrease percent distribution')

                ##emotion analysis
                #df = self.emotion_client.get_score()
                #self.emotion_plot(df, dir_name = '/code/figs', file_name = 'emotion')
                
                all_stock_info = self.rstock_client.get_k_data_in_range(start_date, end_date)
                stm = StrongerThanMarketSelecter()
                stm_code_list = stm.choose(all_stock_info, av_df)

                amus = AntiMarketUpSelecter()
                amus_code_list = amus.choose(today_stock_info)

                gkblvs = GameKLineBigraiseLargeVolumeSelecter()
                gkblvs_code_list = gkblvs.choose(today_stock_info)

                lvhps = LessVolumeHighProfitSelecter()
                #lvhps_code_list = lvhps.choose(today_stock_info)

                ##make dir for new data
                #os.makedirs(dir_name, exist_ok = True)
                #gen review file
                #self.doc.generate(today_stock_info, industry_info, index_info)
                ##gen review animation
                #self.gen_animation()
        except Exception as e:
            self.logger.error(e)

    def gen_animation(self, sfile = None):
        style.use('fivethirtyeight')
        Writer = animation.writers['ffmpeg']
        writer = Writer(fps=1, metadata=dict(artist='biek'), bitrate=1800)
        fig = plt.figure()
        ax = fig.add_subplot(1,1,1)
        _today = datetime.now().strftime('%Y-%m-%d')
        cdata = self.mysql_client.get('select * from %s where date = "%s"' % (ct.ANIMATION_INFO, _today))
        if cdata is None: return None
        cdata = cdata.reset_index(drop = True)
        ctime_list = cdata.time.unique()
        name_list = cdata.name.unique()
        ctime_list = [datetime.strptime(ctime,'%H:%M:%S') for ctime in ctime_list]
        frame_num = len(ctime_list)
        if 0 == frame_num: return None
        def animate(i):
            ax.clear()
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.xaxis.set_major_locator(mdates.DayLocator())
            ax.set_title('盯盘', fontproperties = get_chinese_font())
            ax.set_xlabel('时间', fontproperties = get_chinese_font())
            ax.set_ylabel('增长', fontproperties = get_chinese_font())
            ax.set_ylim((-6, 6))
            fig.autofmt_xdate()
            for name in name_list:
                pchange_list = list()
                price_list = cdata[cdata.name == name]['price'].tolist()
                pchange_list.append(0)
                for _index in range(1, len(price_list)):
                    pchange_list.append(10 * (price_list[_index] - price_list[_index - 1])/price_list[0])
                ax.plot(ctime_list[0:i], pchange_list[0:i], label = name, linewidth = 1.5)
                if pchange_list[i-1] > 1 or pchange_list[i-1] < -1:
                    ax.text(ctime_list[i-1], pchange_list[i-1], name, font_properties = get_chinese_font())
        ani = animation.FuncAnimation(fig, animate, frame_num, interval = 60000, repeat = False)
        sfile = '/data/animation/%s_animation.mp4' % _today if sfile is None else sfile
        ani.save(sfile, writer)
        plt.close(fig)

if __name__ == '__main__':
    creview = CReivew(ct.DB_INFO)
    data = creview.update('2018-12-13')
