#-*- coding: utf-8 -*-
import os
import time
import _pickle
import datetime
import matplotlib
import const as ct
import pandas as pd
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.animation as animation
from cdoc import CDoc
from log import getLogger
from climit import CLimit
from cindex import CIndex
from cmysql import CMySQL
from matplotlib import style
from ccalendar import CCalendar
from datetime import datetime, date
from industry_info import IndustryInfo
from datamanager.margin import Margin
from datamanager.sexchange import StockExchange
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
        self.sh_market_client   = StockExchange(ct.SH_MARKET_SYMBOL)
        self.sz_market_client   = StockExchange(ct.SZ_MARKET_SYMBOL)

    def get_market_info(self):
        df = self.tu_client.index_basic(market = self.SSE)

    def get_stock_data(self):
        df_byte = self.redis.get(ct.TODAY_ALL_STOCK)
        if df_byte is None: return None
        return _pickle.loads(df_byte)

    def get_industry_data(self, _date):
        df = pd.DataFrame()
        df_info = IndustryInfo.get()
        for _, code in df_info.code.iteritems():
            data = CIndex(code).get_k_data(date = _date)
            df = df.append(data)
            df = df.reset_index(drop = True)
        if df.empty: return df
        df['name'] = df_info['name']
        df = df.sort_values(by = 'amount', ascending= False)
        df = df.reset_index(drop = True)
        return df

    def multi_plot(self, x_dict, y_dict, ylabel, title, dir_name, filename):
        fig = plt.figure()
        xlabel = list(x_dict.keys())[0]
        x      = list(x_dict.values())[0]
        xn     = range(len(x))
        plt.xticks(xn, x, rotation = 90)
        plt.xlabel(xlabel, fontproperties = get_chinese_font())
        plt.ylabel(ylabel, fontproperties = get_chinese_font())
        i = 0
        for ylabel, y in y_dict.items():
            i += 1
            plt.plot(xn, y, label = ylabel)
            for xi, yi in zip(xn, y):
                plt.plot((xi,), (yi,), 'ro')
                plt.text(xi, yi, '%s' % yi, fontsize = 7, rotation = 60)
            plt.scatter(xn, y, color = self.COLORS[i], s = 5, marker = "o")
        plt.title(title,    fontproperties = get_chinese_font())
        plt.legend(loc = 'upper right', prop = get_chinese_font())
        fig.autofmt_xdate()
        plt.savefig('%s/%s.png' % (dir_name, filename), dpi=1000)

    def emotion_plot(self, dir_name):
        sql = "select * from %s" % self.emotion_table
        df = self.mysql_client.get(sql)
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
        plt.savefig('%s/emotion.png' % dir_name, dpi=1000)

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

    def static_plot(self, dir_name, stock_info, limit_info):
        limit_up_list   = limit_info[(limit_info.pchange > 0) & (limit_info.prange != 0)].reset_index(drop = True).code.tolist()
        limit_down_list = limit_info[limit_info.pchange < 0].reset_index(drop = True).code.tolist()
        limit_list = limit_up_list + limit_down_list
        changepercent_list = [9, 7, 5, 3, 1, 0, -1, -3, -5, -7, -9]
        num_list = list()
        name_list = list()
        num_list.append(len(limit_up_list))
        name_list.append("涨停")
        c_length = len(changepercent_list)
        for _index in range(c_length):
            pchange = changepercent_list[_index]
            if 0 == _index:
                num_list.append(len(stock_info[(stock_info.changepercent > pchange) & (stock_info.loc[_index, 'code'] not in limit_list)]))
                name_list.append(">%s" % pchange)
            elif c_length - 1 == _index:
                num_list.append(len(stock_info[(stock_info.changepercent < pchange) & (stock_info.loc[_index, 'code'] not in limit_list)]))
                name_list.append("<%s" % pchange)
            else:
                p_max_change = changepercent_list[_index - 1]
                num_list.append(len(stock_info[(stock_info.changepercent > pchange) & (stock_info.changepercent < p_max_change)]))
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
        plt.savefig('%s/static.png' % dir_name, dpi=1000)

    def is_collecting_time(self):
        now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (21,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

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

    def update(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        start_date = get_day_nday_ago(cdate, 30, dformat = "%Y-%m-%d")
        end_date   = cdate
        dir_name = os.path.join(self.sdir, "%s-StockReView" % cdate)
        self.logger.info("create daily info")
        try:
            if not os.path.exists(dir_name):
                self.logger.info("market analysis")
                sh_df = self.get_market_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
                sz_df = self.get_market_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)

                sh_rzrq_df = self.get_rzrq_info(ct.SH_MARKET_SYMBOL, start_date, end_date)
                sz_rzrq_df = self.get_rzrq_info(ct.SZ_MARKET_SYMBOL, start_date, end_date)

                x_dict = dict()
                x_dict['日期'] = sh_df.date.tolist()
                self.market_plot(sh_df, sz_df, x_dict, 'amount')
                self.market_plot(sh_df, sz_df, x_dict, 'negotiable_value')
                self.market_plot(sh_df, sz_df, x_dict, 'number')
                self.market_plot(sh_df, sz_df, x_dict, 'turnover')
                self.market_plot(sh_rzrq_df, sz_rzrq_df, x_dict, 'rzrqye')

                import pdb
                pdb.set_trace()

                #capital analysis
                #price analysis
                #plate analysis
                #plate change analysis
                #marauder map, 板块和个股的活点地图
                #index and total analysis
                index_info = self.get_index_data(_date)
                index_info = index_info.reset_index(drop = True)

                self.get_market_info()


                sh_rzrq_info = rzrq_info.loc[rzrq_info.code == self.SSE]
                sz_rzrq_info = rzrq_info.loc[rzrq_info.code == self.SZSE]

                #index analysis
                    #capital alalysis
                        #流动市值与总成交额(todo not do now)
                            #流动市值分析
                            #总成交额
                        #成交额构成分析
                            #融资融券资金
                            #沪港通资金
                            #涨停板资金
                            #基金仓位资金
                            #股票回购
                            #大宗交易
                        #成交额板块分析
                            #成交额板块排行
                            #成交额增量板块排行
                            #成交额减量板块排行
                            #涨幅排行
                            #跌幅排行
                        #指数点数贡献分析
                            #按照个股排序
                            #按照板块排序
                    #price analysis
                    #emotion alalysis
                        #大盘的情绪分析
                        #抄底模式盈亏
                        #杀跌模式盈亏
                        #追涨模式盈亏
                        #杀多模式盈亏
                #plate alalysis
                    #capital alalysis
                        #沪港通
                        #融资融券
                        #基金
                        #回购
                        #大宗
                    #technical analysis
                        #板块所有股票的活点地图状态
                            #潜龙状态数量
                            #见龙状态数量
                            #飞龙状态数量
                            #亢龙状态数量
                #stock analysis
                    #capital alalysis
                        #沪港通
                        #融资融券
                        #基金
                        #回购
                        #大宗交易
                    #technical analysis
                        #chip alalysis
                            #逆势大盘
                            #90:3
                            #逆势飘红
                            #牛长熊短
                            #线上横盘
                            #博弈K线无量长阳
                            #基础浮动盈利
                #model running
                    #model training
                    #model evaluation 
                    #model backtesting
                    #model trading
                stock_info = self.get_stock_data()
                #get volume > 0 stock list
                stock_info = stock_info[stock_info.volume > 0]
                stock_info = stock_info.reset_index(drop = True)
                #industry analysis
                industry_info = self.get_industry_data(_date)
                if industry_info.empty:
                    self.logger.error("get %s industry info failed" % _date)
                    return
                #limit up and down analysis
                limit_info = self.get_limitup_data(_date)
                # make dir for new data
                os.makedirs(dir_name, exist_ok = True)
                #emotion analysis
                self.emotion_plot(dir_name)
                #static analysis
                self.static_plot(dir_name, stock_info, limit_info)
                #gen review file
                self.doc.generate(stock_info, industry_info, index_info)
                #gen review animation
                self.gen_animation()
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
    data = creview.update('2018-11-12')
