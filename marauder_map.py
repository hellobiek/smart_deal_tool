#coding=utf-8
import os
import sys
import pandas as pd
from cstock import CStock
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation

class MarauderMap():
    def __init__(self, code_list):
        self.codes = code_list
        self.data = self.read()

    def read(self):
        if not os.path.exists('m_data.json'):
            df = pd.DataFrame()
            for code in self.codes:
                tmp_data = CStock(code).get_k_data()
                tmp_data = tmp_data[['cdate', 'profit', 'pday']]
                tmp_data = tmp_data.rename(columns = {"cdate": "time"})
                tmp_data['code'] = code
                df = df.append(tmp_data)
            with open('m_data.json', 'w') as f:
                f.write(df.to_json(orient='records', lines=True))
            sys.exit(0)
        else:
            with open('m_data.json', 'r') as f:
                df = pd.read_json(f.read(), orient = 'records', lines = True, dtype = {'code' : str})
        return df

    def plot(self):
        fig, ax = plt.subplots()

        Writer = animation.writers['ffmpeg']
        writer = Writer(fps=30, metadata=dict(artist='biek'), bitrate=1800)

        #get min profit day
        min_pday = self.data.pday.values.min()
        max_pday = self.data.pday.values.max()
        #get max profit day
        min_profit = self.data.profit.values.min()
        max_profit = self.data.profit.values.max()
        #set axis for map

        xmax = max(abs(min_pday), max_pday)
        ymax = max(abs(min_profit), max_profit)
        groups = list(self.data.groupby(self.data.time))

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
        #ani = animation.FuncAnimation(fig, animate, frames = len(self.data.time), init_func = init, interval = 1, repeat = False)
        sfile = '/code/animation.mp4'
        ani.save(sfile, writer, fps = 60, dpi = 100)
        ax.set_title('Marauder Map for date')
        ax.grid(True)
        plt.close(fig)

if __name__ == '__main__':
    code_list = ['601318']
    mm = MarauderMap(code_list)
    mm.plot()
