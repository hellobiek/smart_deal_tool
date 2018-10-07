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
        writer = Writer(fps=1, metadata=dict(artist='biek'), bitrate=1800)

        #get min profit day
        min_pday = self.data.pday.values.min()
        max_pday = self.data.pday.values.max()
        #get max profit day
        min_profit = self.data.profit.values.min()
        max_profit = self.data.profit.values.max()
        #set axis for map

        xmax = max(abs(min_pday), max_pday)
        ymax = max(abs(min_profit), max_profit)

        def animate(i):
            ax.clear()
            ax.set_xlim(-xmax, xmax)
            ax.set_ylim(-ymax, ymax)
            ax.spines['top'].set_color('none')
            ax.spines['right'].set_color('none')
            ax.xaxis.set_ticks_position('bottom')
            ax.spines['bottom'].set_position(('data', 0))
            ax.yaxis.set_ticks_position('left')
            ax.spines['left'].set_position(('data', 0))
            for cdate, day_data in self.data.groupby(self.data.time):
                for code in self.codes:
                    pday   = day_data.loc[day_data.code == code, 'pday'].values[0]
                    profit = day_data.loc[day_data.code == code, 'profit'].values[0]
                    ax.scatter(pday, profit, alpha = 1)

        ani = animation.FuncAnimation(fig, animate, frames=len(self.data.time), interval = 1000, repeat = False)
        sfile = '/code/animation.mp4'
        ani.save(sfile, writer)
        ax.set_title('Marauder Map for date')
        ax.grid(True)
        plt.close(fig)

if __name__ == '__main__':
    code_list = ['601318']
    mm = MarauderMap(code_list)
    mm.plot()
