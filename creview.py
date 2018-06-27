#-*- coding: utf-8 -*-
import os
import time
import json
import _pickle
import datetime
from datetime import datetime, date
import traceback
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
import matplotlib
matplotlib.use('Agg')
from matplotlib import style
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.dates as mdates
import matplotlib.animation as animation
from matplotlib.font_manager import FontProperties
from cmysql import CMySQL
from cdoc import CDoc
from common import create_redis_obj, get_redis_name, is_trading_time

def get_chinese_font():
    return FontProperties(fname='/conf/fonts/PingFang.ttc')

class CReivew:
    def __init__(self, dbinfo):
        self.sdir = '/data/docs/blog/hellobiek.github.io/source/_posts'
        self.doc = CDoc(self.sdir)
        self.animate_started = False
        self.redis = create_redis_obj()
        self.mysql_client = CMySQL(dbinfo)
        self.trading_info = ts.get_today_all()
        self.emotion_table = ct.EMOTION_TABLE
        self.industry_table = ct.INDUSTRY_TABLE 
        if not self.create_industry(): raise Exception("create industry table failed")
        if not self.create_emotion(): raise Exception("create emotion table failed")

    def get_industry_name_dict_from_tongdaxin(self, fname):
        industry_dict = dict()
        with open(fname, "rb") as f:
            data = f.read()
        info_list = data.decode("gbk").split('######\r\n')
        for info in info_list:
            xlist = info.split('\r\n')
            if xlist[0] == '#TDXNHY':
                zinfo = xlist[1:len(xlist)-1]
        for z in zinfo:
            x = z.split('|')
            industry_dict[x[0]] = x[1]
        return industry_dict

    def get_industry_code_dict_from_tongdaxin(self, fname):
        industry_dict = dict()
        with open(fname, "rb") as f:
            data = f.read()
        str_list = data.decode("utf-8").split('\r\n')
        for x in str_list:
            info_list = x.split('|')
            if len(info_list) == 4:
                industry = info_list[2]
                code = info_list[1]
                if industry == "T00": continue #not include B stock
                if industry not in industry_dict: industry_dict[industry] = list()
                industry_dict[industry].append(code)
        for key in industry_dict:
            industry_dict[key] = json.dumps(industry_dict[key])
        return industry_dict

    def get_industry(self):
        industry_code_dict = self.get_industry_code_dict_from_tongdaxin(ct.TONG_DA_XIN_CODE_PATH)
        industry_name_dict = self.get_industry_name_dict_from_tongdaxin(ct.TONG_DA_XIN_INDUSTRY_PATH)
        name_list = list()
        for key in industry_code_dict:
            name_list.append(industry_name_dict[key])
        data = {'name':name_list, 'code':list(industry_code_dict.keys()), 'content':list(industry_code_dict.values())}
        return pd.DataFrame.from_dict(data)

    def create_industry(self):
        if self.industry_table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, code varchar(10) not null, name varchar(20), amount float(4), PRIMARY KEY (date, code))' % self.industry_table 
            if not self.mysql_client.create(sql, self.industry_table): return False
        return True

    def create_emotion(self):
        if self.emotion_table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, score float(2) not null, PRIMARY KEY (date, score))' % self.emotion_table 
            if not self.mysql_client.create(sql, self.emotion_table): return False
        return True

    def collect_industry_info(self):
        industry_df = self.get_industry()
        name_list = list()
        icode_list = list()
        changepercent_list = list()
        turnoverratio_list = list()
        amount_list = list()
        for index, code_id in industry_df['code'].items():
            code_list = json.loads(industry_df.loc[index]['content'])
            code_info = self.trading_info[self.trading_info.code.isin(code_list)]
            _name = industry_df.loc[index]['name']
            name_list.append(_name)
            icode_list.append(code_id)
            _amount = code_info.amount.astype(float).sum()
            amount_list.append(_amount)

        data = {'name':name_list, 'code':icode_list, 'amount': amount_list}
        df = pd.DataFrame.from_dict(data)
        df['date'] = datetime.now().strftime('%Y-%m-%d')

        if not self.mysql_client.set(df, self.industry_table):
            raise Exception("set data to industry failed")

    def gen_today_industry(self):
        sql = "select * from %s where date = '%s';" % (self.industry_table, datetime.now().strftime('%Y-%m-%d'))
        df = self.mysql_client.get(sql)
        df = df[['amount', 'name']]
        df = df.sort_values(by = 'amount', ascending= False)
        total_amount = df.amount.astype(float).sum()

        df = df[0:10]
        most_amount = df.amount.astype(float).sum()
        other_amount = total_amount - most_amount 
        df.loc[len(df)] = [other_amount, '其他']
        df = df.sort_values(by = 'amount', ascending= False)
        return df.reset_index(drop = True)

    def emotion_plot(self, dir_name):
        sql = "select * from %s;" % self.emotion_table
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
        plt.savefig('%s/emotion.png' % dir_name, dpi=1000)

    def industry_plot(self, df, dir_name):
        colors = ['b', 'r', 'y', 'g', 'm']
        fig = plt.figure()
    
        amount_sum = 0
        amount_list = df.amount.tolist()
    
        for amount in amount_list:
            amount_sum += amount
    
        x = date.fromtimestamp(time.time())
        plt.bar(x, amount_sum, color = 'c', width=0.1)
        plt.text(x , amount_sum * 0.95 , '总成交额:%s亿' % (amount_sum / 100000000), ha='center', font_properties = get_chinese_font())
    
        plot_amount = amount_sum
        plot_amount_list = list()
    
        for amount in amount_list:
            plot_amount = plot_amount - amount
            plot_amount_list.append(plot_amount)
    
        for i in range(len(plot_amount_list)):
            y = plot_amount_list[i]
            plt.bar(x, y, color = colors[i % len(colors)], width=0.1)
            per_y = "{:.4%}".format(df.loc[i]['amount']/amount_sum)
            plt.text(x, 1.1 * y, '%s占比:%s' % (df.loc[i]['name'], per_y), ha='center', font_properties = get_chinese_font())
    
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.xlabel('x轴', fontproperties = get_chinese_font())
        plt.ylabel('y轴', fontproperties = get_chinese_font())
        plt.title('市值分布', fontproperties = get_chinese_font())
        plt.savefig('%s/industry.png' % dir_name, dpi=1000)

    def gen_market_emotion_score(self):
        total = 0
        changepercent_list = self.trading_info.changepercent.tolist()
        for changepercent in changepercent_list:
            if changepercent > 11: continue
            elif changepercent > 9.87 or changepercent < -9.92: total += changepercent * 2
            else: total += changepercent

        data = {'date':["%s" % datetime.now().strftime('%Y-%m-%d')], 'score':[total]}
        df = pd.DataFrame.from_dict(data)
        if not self.mysql_client.set(df, self.emotion_table):
            raise Exception("set data to emotion failed")

    def static_plot(self, dir_name):
        colors = ['b', 'r', 'y', 'g', 'm']
        num_list = list()
        changepercent_list = [9.81, 5, 3, 1, 0, -1, -3, -5, -9.91]
        name_list = list()
        c_length = len(changepercent_list)
        for _index in range(c_length):
            pchange = changepercent_list[_index]
            if 0 == _index:
                num_list.append(len(self.trading_info[self.trading_info.changepercent > pchange]))
                name_list.append(">%s" % pchange)
            elif c_length - 1 == _index:
                num_list.append(len(self.trading_info[self.trading_info.changepercent < pchange]))
                name_list.append("<%s" % pchange)
            else:
                p_max_change = changepercent_list[_index - 1]
                num_list.append(len(self.trading_info[(self.trading_info.changepercent > pchange) & (self.trading_info.changepercent < p_max_change)]))
                name_list.append("%s-%s" % (pchange, p_max_change))
    
        fig = plt.figure()
        for i in range(len(num_list)):
            plt.bar(i, num_list[i], color = colors[i % len(colors)], width=0.1)
            plt.text(i, 1.1 * num_list[i], '个数:%s' % num_list[i], ha='center', font_properties = get_chinese_font())
    
        plt.xlabel('x轴', fontproperties = get_chinese_font())
        plt.ylabel('y轴', fontproperties = get_chinese_font())
        plt.title('涨跌分布', fontproperties = get_chinese_font())
        plt.xticks(range(len(num_list)), name_list)
        plt.savefig('%s/static.png' % dir_name, dpi=1000)
   
    def get_combination_dict(self):
        df_byte = self.redis.get(ct.COMBINATION_INFO)
        if df_byte is None: return None 
        df =  _pickle.loads(df_byte)
        cdict = dict()
        for _index, code in df['code'].items():
            cdict[code] = df.loc[_index]['name']
        return cdict

    def is_collecting_time(self):
        now_time = datetime.now()
        _date = now_time.strftime('%Y-%m-%d')
        y,m,d = time.strptime(_date, "%Y-%m-%d")[0:3]
        mor_open_hour,mor_open_minute,mor_open_second = (15,0,0)
        mor_open_time = datetime(y,m,d,mor_open_hour,mor_open_minute,mor_open_second)
        mor_close_hour,mor_close_minute,mor_close_second = (23,59,59)
        mor_close_time = datetime(y,m,d,mor_close_hour,mor_close_minute,mor_close_second)
        return mor_open_time < now_time < mor_close_time

    def update(self, sleep_time):
        while True:
            try:
                if self.is_collecting_time():
                    _date = datetime.now().strftime('%Y-%m-%d')
                    dir_name = os.path.join(self.sdir, "%s-StockReView" % _date)
                    if not os.path.exists(dir_name):
                        os.makedirs(dir_name)
                        self.collect_industry_info()
                        df = self.gen_today_industry()
                        self.industry_plot(df, dir_name)
                        self.gen_market_emotion_score()
                        self.emotion_plot(dir_name)
                        self.static_plot(dir_name)
                        self.doc.generate()
                time.sleep(sleep_time)
            except Exception as e:
                traceback.print_exc()

    def run(self, sleep_time):
        while True:
            try:
                if is_trading_time() and False == self.animate_started:
                    self.animate_started = True
                    self.review_animate()
                time.sleep(sleep_time)
            except Exception as e:
                traceback.print_exc()

    def review_animate(self):
        def condition():
            while is_trading_time():
                yield 0
            self.animate_started = False

        time_list = list()
        data_dict = {'上证指数':list()}
        def animate(i):
            cdict = self.get_combination_dict()
            if len(cdict) > 0:
                df = ts.get_realtime_quotes('sh')
                p_change = 100 * (float(df.price.tolist()[0]) - float(df.pre_close.tolist()[0]))/float(df.pre_close.tolist()[0])
                data_dict['上证指数'].append(p_change)
                now_time = time.mktime(datetime.now().timetuple())
                time_list.append(now_time)
                for code in cdict:
                    key = cdict[code]
                    if key not in data_dict: data_dict[key] = list()
                    df_byte = self.redis.get(get_redis_name(code))
                    df = _pickle.loads(df_byte)
                    p_change = 100 * (float(df.price.tolist()[0]) - float(df.pre_close.tolist()[0])) / float(df.pre_close.tolist()[0])
                    data_dict[key].append(p_change)
                ax1.clear()
                plt.title('盯盘', fontproperties=get_chinese_font())
                plt.xlabel('时间', fontproperties=get_chinese_font())
                plt.ylabel('增长', fontproperties=get_chinese_font())
                plt.ylim((-10, 50))
                _index = len(time_list) - 1
                for key in data_dict:
                    ax1.plot(time_list, data_dict[key], label = key, linewidth = 1.5)
                    if data_dict[key][_index] > 5.0:
                        ax1.text(time_list[_index], data_dict[key][_index]*4, key, font_properties = get_chinese_font())
                ax1.legend(fontsize = 'xx-small', bbox_to_anchor = (1.0, 1.0), ncol = 7, fancybox = True, prop = get_chinese_font())

        style.use('fivethirtyeight')
        Writer = animation.writers['ffmpeg']
        writer = Writer(fps=1, metadata=dict(artist='biek'), bitrate=1800)
        fig = plt.figure()
        ax1 = fig.add_subplot(1,1,1)
        ani = animation.FuncAnimation(fig, animate, frames = condition, interval = 60000, repeat = False)
        ani.save('/data/animation/%s_animation.mp4' % datetime.now().strftime('%Y-%m-%d'), writer = writer)

if __name__ == '__main__':
    cr = CReivew(ct.STAT_INFO)
    cr.update(3)
