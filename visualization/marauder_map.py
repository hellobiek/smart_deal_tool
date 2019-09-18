#-*- coding: utf-8 -*-
import os
import sys
import time
import copy
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import matplotlib
import matplotlib.pyplot as plt
from rstock import RIndexStock
from base.clog import getLogger
from common import get_chinese_font
from base.cdate import get_day_nday_ago
class MarauderMap():
    def __init__(self):
        self.ris = RIndexStock()
        self.logger = getLogger(__name__)

    def get_data(self, mdate):
        return self.ris.get_data(mdate)

    def plot(self, cdate, fdir, fname):
        df = self.ris.get_data(cdate)
        if df.empty: return
        fig, ax = plt.subplots()
        #get min profit day
        min_pday = df.pday.values.min()
        max_pday = df.pday.values.max()
        #get max profit day
        min_profit = df.profit.values.min()
        max_profit = df.profit.values.max()
        #set axis for map
        xmax = max(abs(min_pday), max_pday)
        ymax = max(abs(min_profit), max_profit)

        ax.set_xlim(-xmax, xmax)
        ax.set_ylim(-ymax, ymax)
        ax.spines['top'].set_color('none')
        ax.spines['right'].set_color('none')
        ax.xaxis.set_ticks_position('bottom')
        ax.spines['bottom'].set_position(('data', 0))
        ax.yaxis.set_ticks_position('left')
        ax.spines['left'].set_position(('data', 0))
        for code in df.code.tolist():
            pday   = df.loc[df.code == code, 'pday']
            profit = df.loc[df.code == code, 'profit']
            ax.scatter(pday, profit, s = 5, alpha = 1, linewidths = 0.1)
        plt.savefig('%s/%s.png' % (fdir, fname), dpi=1000)

    def gen_animation(self, end_date, days):
        import matplotlib.animation as animation
        matplotlib.use('Agg')
        start_date = get_day_nday_ago(end_date, num = days, dformat = "%Y-%m-%d")
        df = self.ris.get_k_data_in_range(start_date, end_date)
        fig, ax = plt.subplots()
        #get min profit day
        min_pday = df.pday.values.min()
        max_pday = df.pday.values.max()
        #get max profit day
        min_profit = df.profit.values.min()
        max_profit = df.profit.values.max()
        #set axis for map
        xmax = max(abs(min_pday), max_pday)
        ymax = max(abs(min_profit), max_profit)
        groups = df.groupby(df.date)
        dates = list(set(df.date.tolist()))
        dates.sort()
        Writer = animation.writers['ffmpeg']
        writer = Writer(fps = 2, metadata = dict(artist='biek'), bitrate = -1)
        def init():
            ax.clear()
            ax.set_xlim(-xmax, xmax)
            ax.set_ylim(-ymax, ymax)
            ax.spines['top'].set_color('none')
            ax.spines['right'].set_color('none')
            ax.xaxis.set_ticks_position('bottom')
            ax.spines['bottom'].set_position(('data', 0))
            ax.yaxis.set_ticks_position('left')
            ax.spines['left'].set_position(('data', 0))
           
        def animate(i):
            cdate = dates[i]
            df = groups.get_group(cdate)
            init()
            print(cdate, len(df))
            bull_stock_num = len(df[df.profit >= 0])
            for code in df.code.tolist():
                pday   = df.loc[df.code == code, 'pday']
                profit = df.loc[df.code == code, 'profit']
                ax.scatter(pday, profit, color = 'black', s = 1)
                ax.set_title("日期：%s 股票总数：%s 牛熊股比:%s" % (cdate, len(df), 100 * bull_stock_num / len(df)), fontproperties = get_chinese_font())

        ani = animation.FuncAnimation(fig, animate, frames = len(dates), init_func = init, interval = 1000, repeat = False)
        sfile = '/code/panimation.mp4'
        ani.save(sfile, writer)
        ax.set_title('Marauder Map for date')
        ax.grid(True)
        plt.close(fig)

if __name__ == '__main__':
    cdate = "2019-09-17"
    mmap_clinet = MarauderMap()
    mmap_clinet.get_data(mdate)
    #mmap_clinet.plot(cdate, image_dir, image_name)
    #mmap_clinet.gen_animation("2019-01-25", 100)
