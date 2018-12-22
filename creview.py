#-*- coding: utf-8 -*-
import gevent
from gevent import monkey
monkey.patch_all(thread = True)
from gevent.pool import Pool
import os
import datetime
import matplotlib
import const as ct
import pandas as pd
matplotlib.use('Agg')
from matplotlib import style
import matplotlib.dates as mdates
import matplotlib.animation as animation
from cdoc import CDoc
from log import getLogger
from climit import CLimit
from cindex import CIndex
from cmysql import CMySQL
from datetime import datetime
from rstock import RIndexStock
import matplotlib.pyplot as plt
from datamanager.margin import Margin
from rindustry import RIndexIndustryInfo
from industry_info import IndustryInfo
from datamanager.emotion import Emotion
from datamanager.sexchange import StockExchange
from common import create_redis_obj, get_chinese_font, get_tushare_client, get_day_nday_ago
class CReivew:
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo             = dbinfo
        self.logger             = getLogger(__name__)
        self.tu_client          = get_tushare_client()
        self.doc                = CDoc()
        self.redis              = create_redis_obj() if redis_host is None else create_redis_obj(redis_host)
        self.mysql_client       = CMySQL(dbinfo, iredis = self.redis)
        self.margin_client      = Margin(dbinfo = dbinfo, redis_host = redis_host)
        self.rstock_client      = RIndexStock(dbinfo = dbinfo, redis_host = redis_host) 
        self.sh_market_client   = StockExchange(ct.SH_MARKET_SYMBOL)
        self.sz_market_client   = StockExchange(ct.SZ_MARKET_SYMBOL)
        self.emotion_client     = Emotion()

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

    def get_limitup_data(self, cdate):
        return CLimit(self.dbinfo).get_data(cdate)

    def get_index_data(self, cdate):
        df = pd.DataFrame()
        for code, name in ct.TDX_INDEX_DICT.items():
            data = CIndex(code).get_k_data(cdate)
            data['name'] = name
            data['code'] = code
            df = df.append(data)
        df = df.reset_index(drop = True)
        return df

    def get_market_data(self, market, start_date, end_date):
        if market == ct.SH_MARKET_SYMBOL:
            df = self.sh_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '上海市场']
        else:
            df = self.sz_market_client.get_k_data_in_range(start_date, end_date)
            df = df.loc[df.name == '深圳市场']
        df = df.round(2)
        df = df.drop_duplicates()
        df = df.reset_index(drop = True)
        df = df.sort_values(by = 'date', ascending= True)
        df.negotiable_value = (df.negotiable_value / 2).astype(int)
        return df

    def get_rzrq_info(self, market, start_date, end_date):
        df = self.margin_client.get_k_data_in_range(start_date, end_date)
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

    def get_index_df(self, code, start_date, end_date):
        cindex_client = CIndex(code)
        df = cindex_client.get_k_data_in_range(start_date, end_date)
        df['time'] = df.index.tolist()
        df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
        return df

    def update(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        start_date = get_day_nday_ago(cdate, 100, dformat = "%Y-%m-%d")
        end_date   = cdate
        try:
            #market info
            sh_df = self.get_market_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
            sz_df = self.get_market_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)
            #rzrq info
            sh_rzrq_df = self.get_rzrq_info(ct.SH_MARKET_SYMBOL, start_date, end_date)
            sz_rzrq_df = self.get_rzrq_info(ct.SZ_MARKET_SYMBOL, start_date, end_date)
            #average price info
            av_df = self.get_index_df('880003', start_date, end_date)
            #limit up and down info
            limit_info = self.get_limitup_data(cdate)
            stock_info = self.rstock_client.get_data(cdate)
            stock_info = stock_info[stock_info.volume > 0] #get volume > 0 stock list
            stock_info = stock_info.reset_index(drop = True)
            #index info
            index_info = self.get_index_data(end_date)
            #industry analysis
            industry_info = self.get_industry_data(cdate)
            #all stock info 
            all_stock_info = self.rstock_client.get_k_data_in_range(start_date, end_date)
            #gen review file and make dir for new data
            self.doc.generate(cdate, sh_df, sz_df, sh_rzrq_df, sz_rzrq_df, av_df, limit_info, stock_info, industry_info, index_info, all_stock_info)
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
