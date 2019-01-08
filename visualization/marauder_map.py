#-*- coding: utf-8 -*-
import os
import sys
import time
import copy
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import pandas as pd
from cstock import CStock
from rstock import RIndexStock
from log import getLogger
from functools import partial
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation

class MarauderMap():
    def __init__(self, code_list):
        self.codes  = code_list
        self.ris = RIndexStock()
        self.logger = getLogger(__name__)

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

        for code in self.codes:
            pday   = df.loc[df.code == code, 'pday']
            profit = df.loc[df.code == code, 'profit']
            ax.scatter(pday, profit, s = 5, alpha = 1, linewidths = 0.1)
        plt.savefig('%s/%s.png' % (fdir, fname), dpi=1000)

    def gen_animation(self, cdate):
        df = self.get_data(cdate)
        fig, ax = plt.subplots()

        Writer = animation.writers['ffmpeg']
        writer = Writer(fps=30, metadata=dict(artist='biek'), bitrate=1800)

        #get min profit day
        min_pday = df.pday.values.min()
        max_pday = df.pday.values.max()
        #get max profit day
        min_profit = df.profit.values.min()
        max_profit = df.profit.values.max()
        #set axis for map

        xmax = max(abs(min_pday), max_pday)
        ymax = max(abs(min_profit), max_profit)
        groups = list(df.groupby(df.time))

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
            
        def animate(n):
            val = groups[n][1].values.tolist()[0]
            for code in self.codes:
                pday   = val[1] 
                profit = val[2] 
                ax.scatter(pday, profit, s=5, alpha = 1, linewidths = 0.1)

        ani = animation.FuncAnimation(fig, animate, frames = 300, init_func = init, interval = 1, repeat = False)
        sfile = '/code/animation.mp4'
        ani.save(sfile, writer, fps = 60, dpi = 100)
        ax.set_title('Marauder Map for date')
        ax.grid(True)
        plt.close(fig)
