#-*- coding: utf-8 -*-
import gevent
from gevent import monkey
monkey.patch_all(thread=True)
from gevent.pool import Pool
from gevent.lock import Semaphore
from cgreent import CGreenlet
import os
import json
import time
import _pickle
import datetime
from datetime import datetime, date
from os import path
import const as ct
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
from matplotlib import style
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import matplotlib.animation as animation
from matplotlib.collections import LineCollection
from matplotlib.font_manager import FontProperties
from cdoc import CDoc
from cmysql import CMySQL
from cstock import CStock
from cindex import CIndex
from climit import CLimit
from functools import partial
from cstock_info import CStockInfo 
from industry_info import IndustryInfo
from sklearn import cluster, covariance, manifold, preprocessing
import ccalendar
from common import create_redis_obj, get_dates_array
from log import getLogger
from hurst import compute_Hc
import statsmodels.api as sm
import statsmodels.tsa.stattools as ts
logger = getLogger(__name__)
def get_chinese_font():
    return FontProperties(fname='/conf/fonts/PingFang.ttc')

class CReivew:
    def __init__(self, dbinfo):
        self.dbinfo = dbinfo
        self.sdir = '/data/docs/blog/hellobiek.github.io/source/_posts'
        self.doc = CDoc(self.sdir)
        self.stock_objs = dict()
        self.lock = Semaphore(1)
        self.redis = create_redis_obj()
        self.mysql_client = CMySQL(self.dbinfo)
        self.cal_client = ccalendar.CCalendar(without_init = True)
        self.animating = False
        self.emotion_table = ct.EMOTION_TABLE
        if not self.create_emotion(): raise Exception("create emotion table failed")

    def create_emotion(self):
        if self.emotion_table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null, score float, PRIMARY KEY (date))' % self.emotion_table 
            if not self.mysql_client.create(sql, self.emotion_table): return False
        return True

    def get_today_all_stock_data(self, _date):
        df_byte = self.redis.get(ct.TODAY_ALL_STOCK)
        if df_byte is None: return None
        df = _pickle.loads(df_byte)
        return df[df.date == _date]

    def get_industry_data(self, _date):
        df = pd.DataFrame()
        df_info = IndustryInfo.get()
        for _, code in df_info.code.iteritems():
            data = CIndex(self.dbinfo, code).get_k_data(date = _date)
            df = df.append(data)
            df = df.reset_index(drop = True)
        df['name'] = df_info['name']
        df = df.sort_values(by = 'amount', ascending= False)
        df = df.reset_index(drop = True)
        return df

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
        #colors = ['#F5DEB3', '#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082', '#ADD8E6']
        colors = ['#F5DEB3', '#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082']
        industry_info.amount = industry_info.amount / 10000000000
        total_amount = industry_info.amount.sum()
        amount_list = industry_info[0:10].amount.tolist()
        x = date.fromtimestamp(time.time())
        fig = plt.figure()
        base_line = 0 
        for i in range(len(amount_list)):
            label_name = "%s:%s" % (industry_info.loc[i]['name'], 100 * amount_list[i] / total_amount)
            plt.bar(x, amount_list[i], width = 0.1, color = colors[i], bottom = base_line, align = 'center', label = label_name)
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

    def gen_market_emotion_score(self, stock_info, limit_info):
        limit_up_list = limit_info[(limit_info.pchange > 0) & (limit_info.prange != 0)].reset_index(drop = True).code.tolist()
        limit_down_list = limit_info[limit_info.pchange < 0].reset_index(drop = True).code.tolist()
        limit_up_list.extend(limit_down_list)
        total = 0
        for _index, pchange in stock_info.changepercent.iteritems():
            code = str(stock_info.loc[_index, 'code']).zfill(6)
            if code in limit_up_list: 
                total += 2 * pchange
            else:
                total += pchange
        aver = total / len(stock_info)
        data = {'date':["%s" % datetime.now().strftime('%Y-%m-%d')], 'score':[aver]}
        df = pd.DataFrame.from_dict(data)
        if not self.mysql_client.set(df, self.emotion_table):
            raise Exception("set data to emotion failed")

    def static_plot(self, dir_name, stock_info, limit_info):
        colors = ['b', 'r', 'y', 'g', 'm']
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
            plt.bar(i + 1, num_list[i], color = colors[i % len(colors)], width = 0.3)
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
            data = self.mysql_client.get("select * from day where cdate=\"%s\";" % _date)
            data['name'] = name
            df = df.append(data)
        self.mysql_client.changedb()
        return df

    def update(self):
        _date = datetime.now().strftime('%Y-%m-%d')
        dir_name = os.path.join(self.sdir, "%s-StockReView" % _date)
        try:
            if not os.path.exists(dir_name):
                logger.info("create daily info")
                #stock analysis
                stock_info = self.get_today_all_stock_data(_date)
                #get volume > 0 stock list
                stock_info = stock_info[stock_info.volume > 0]
                stock_info = stock_info.reset_index(drop = True)
                os.makedirs(dir_name, exist_ok = True)
                #industry analysis
                industry_info = self.get_industry_data(_date)
                #index and total analysis
                index_info = self.get_index_data(_date)
                index_info = index_info.reset_index(drop = True)
                #limit up and down analysis
                limit_info = self.get_limitup_data(_date)
                #emotion analysis
                self.gen_market_emotion_score(stock_info, limit_info)
                self.emotion_plot(dir_name)
                #static analysis
                self.static_plot(dir_name, stock_info, limit_info)
                #gen review file
                self.doc.generate(stock_info, industry_info, index_info)
                #gen review animation
                self.gen_animation()
        except Exception as e:
            logger.error(e)

    def gen_animation(self, sfile = None):
        style.use('fivethirtyeight')
        Writer = animation.writers['ffmpeg']
        writer = Writer(fps=1, metadata=dict(artist='biek'), bitrate=1800)
        fig = plt.figure()
        ax = fig.add_subplot(1,1,1)
        _today = datetime.now().strftime('%Y-%m-%d')
        cdata = self.mysql_client.get('select * from %s where cdate = "%s"' % (ct.ANIMATION_INFO, _today))
        if cdata is None: return None
        cdata = cdata.reset_index(drop = True)
        ctime_list = cdata.ctime.unique()
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

    def get_range_data(self, code, start_date, end_date):
        sql = "select * from day where cdate between \"%s\" and \"%s\"" %(start_date, end_date)
        self.mysql_client.changedb(CStock.get_dbname(code))
        return self.mysql_client.get(sql)

    def gen_stocks_trends(self, start_date, end_date, stock_info):
        greenlets = list()
        logger.info("read start")
        obj_pool = Pool(300)
        for code in stock_info.code:
            g = CGreenlet(code, self.get_range_data, code, start_date, end_date)
            greenlets.append(g)
            obj_pool.start(g)
        obj_pool.join(timeout = 180)
        obj_pool.kill()
        self.mysql_client.changedb()
        logger.info("read succeed")
        all_df = pd.DataFrame()
        while(len(greenlets) > 0):
            for i in range(len(greenlets) - 1, -1, -1):
                g = greenlets[i]
                if g.ready():
                    if g.value is not None:
                        tmp_df = g.value
                        if not tmp_df.empty:
                            tmp_df['code'] = g.name
                            all_df = all_df.append(tmp_df)
                    else:
                        logger.info("%s restart" % g.name)
                        g_new = CGreenlet(g.name, self.get_range_data, g.name, start_date, end_date)
                        g_new.start()
                        g_new.join(timeout = 6)
                        greenlets.append(g_new)
                    del greenlets[i]
        logger.info("dataframe succeed")
        return all_df

    def relation_plot(self, df, good_list):
        close_price_list = [df[df.code == code].close.tolist() for code in good_list]
        close_prices = np.vstack(close_price_list)
    
        open_price_list = [df[df.code == code].open.tolist() for code in good_list]
        open_prices = np.vstack(open_price_list)
    
        # the daily variations of the quotes are what carry most information
        variation = (close_prices - open_prices) * 100 / open_prices
    
        logger.info("get variation succeed")
        # #############################################################################
        # learn a graphical structure from the correlations
        edge_model = covariance.GraphLassoCV()
        # standardize the time series: using correlations rather than covariance is more efficient for structure recovery
        X = variation.copy().T
        X /= X.std(axis = 0)
        edge_model.fit(X)
    
        logger.info("mode compute succeed")
        # #############################################################################
        # cluster using affinity propagation
        _, labels = cluster.affinity_propagation(edge_model.covariance_)
        n_labels = labels.max()
        code_list = np.array(good_list)
    
        industry_dict = dict()
        industry_df_info = IndustryInfo.get()
        for index, name in industry_df_info.name.iteritems():
            content = industry_df_info.loc[index]['content']
            a_code_list = json.loads(content)
            for code in a_code_list:
                industry_dict[code] = name
    
        cluster_dict = dict()
        for i in range(n_labels + 1):
            cluster_dict[i] = code_list[labels == i]
            name_list = [CStockInfo.get(code, 'name') for code in code_list[labels == i]]
            logger.info('cluster code %i: %s' % ((i + 1), ', '.join(name_list)))
    
        cluster_info = dict()
        for group, _code_list in cluster_dict.items():
            for code in _code_list:
                iname = industry_dict[code]
                if group not in cluster_info: cluster_info[group] = set()
                cluster_info[group].add(iname)
            logger.info('cluster inustry %i: %s' % ((i + 1), ', '.join(list(cluster_info[group]))))
    
        # #############################################################################
        # find a low-dimension embedding for visualization: find the best position of
        # the nodes (the stocks) on a 2D plane
        # we use a dense eigen_solver to achieve reproducibility (arpack is
        # initiated with random vectors that we don't control). In addition, we
        # use a large number of neighbors to capture the large-scale structure.
        node_position_model = manifold.LocallyLinearEmbedding(n_components=2, eigen_solver='dense', n_neighbors=6)
        embedding = node_position_model.fit_transform(X.T).T
    
        # #############################################################################
        # visualizatio
        plt.figure(1, facecolor='w', figsize=(10, 8))
        plt.clf()
        ax = plt.axes([0., 0., 1., 1.])
        plt.axis('off')
    
        # display a graph of the partial correlations
        partial_correlations = edge_model.precision_.copy()
        d = 1 / np.sqrt(np.diag(partial_correlations))
        partial_correlations *= d
        partial_correlations *= d[:, np.newaxis]
        non_zero = (np.abs(np.triu(partial_correlations, k=1)) > 0.02)
    
        # plot the nodes using the coordinates of our embedding
        plt.scatter(embedding[0], embedding[1], s=100 * d ** 2, c = labels, cmap=plt.cm.nipy_spectral)
    
        # plot the edges
        start_idx, end_idx = np.where(non_zero)
        # a sequence of (*line0*, *line1*, *line2*), where:: linen = (x0, y0), (x1, y1), ... (xm, ym)
        segments = [[embedding[:, start], embedding[:, stop]] for start, stop in zip(start_idx, end_idx)]
        values = np.abs(partial_correlations[non_zero])
        lc = LineCollection(segments, zorder=0, cmap=plt.cm.hot_r, norm=plt.Normalize(0, .7 * values.max()))
        lc.set_array(values)
        lc.set_linewidths(15 * values)
        ax.add_collection(lc)
    
        # add a label to each node. The challenge here is that we want to position the labels to avoid overlap with other labels
        for index, (name, label, (x, y)) in enumerate(zip(code_list, labels, embedding.T)):
            dx = x - embedding[0]
            dx[index] = 1
            dy = y - embedding[1]
            dy[index] = 1
            this_dx = dx[np.argmin(np.abs(dy))]
            this_dy = dy[np.argmin(np.abs(dx))]
            if this_dx > 0:
                horizontalalignment = 'left'
                x = x + .002
            else:
                horizontalalignment = 'right'
                x = x - .002
            if this_dy > 0:
                verticalalignment = 'bottom'
                y = y + .002
            else:
                verticalalignment = 'top'
                y = y - .002
            plt.text(x, y, name, size=10, horizontalalignment=horizontalalignment, verticalalignment=verticalalignment, bbox=dict(facecolor='w', edgecolor=plt.cm.nipy_spectral(label / float(n_labels)), alpha=.6))
        plt.xlim(embedding[0].min() - .15 * embedding[0].ptp(), embedding[0].max() + .10 * embedding[0].ptp(),)
        plt.ylim(embedding[1].min() - .03 * embedding[1].ptp(), embedding[1].max() + .03 * embedding[1].ptp())
        plt.savefig('/tmp/relation.png', dpi=1000)
   
    def plot_price_series(self, df, ts1, ts2):
        fig = plt.figure()
        x = df.loc[df.code == ts1].cdate.tolist()
        xn = range(len(x))
        y1 = df.loc[df.code == ts1].close.tolist()
        name1 = df[df.code == ts1].name.values[0]
        name2 = df[df.code == ts2].name.values[0]
        y2 = df.loc[df.code == ts2].close.tolist()
        plt.plot(xn, y1, label = name1, linewidth = 1.5)
        plt.plot(xn, y2, label = name2, linewidth = 1.5)
        plt.xticks(xn, x)
        plt.xlabel('时间', fontproperties = get_chinese_font())
        plt.ylabel('分数', fontproperties = get_chinese_font())
        plt.title('协整关系', fontproperties = get_chinese_font())
        fig.autofmt_xdate()
        plt.legend(loc = 'upper right', prop = get_chinese_font())
        plt.savefig('/tmp/relation/%s_%s.png' % (ts1, ts2), dpi=1000)
        plt.close(fig)

def choose_stock(df, code):
    p_df = df[df.code ==  code]
    median_value = np.median(p_df.amount)
    return code if median_value > MONEY_LIMIT else None

def data_std(df, _date):
    tmp_df = df.loc[df.cdate == _date, 'pchange']
    x = preprocessing.scale(tmp_df)
    return pd.Series(x, index = tmp_df.index)

def adjust_name_and_pchange(df, stock_info, values, code):
    #get tmp df
    tmp_df = df.loc[df.code == code, 'pchange']
    #set name
    name = stock_info[stock_info.code == code].name.values[0]
    names = [name for n in range(len(tmp_df))]
    name_series = pd.Series(names, index = tmp_df.index)
    #set industry
    industry = stock_info[stock_info.code == code].industry.values[0]
    industries = [industry for n in range(len(tmp_df))]
    industry_series = pd.Series(industries, index = tmp_df.index)
    #set pchange
    pchange_custom = pd.Series(values, index = tmp_df.index)
    new_tmp_df = pd.DataFrame()
    new_tmp_df['name'] = name_series
    new_tmp_df['industry'] = industry_series
    new_tmp_df['pchange'] = tmp_df - pchange_custom
    return new_tmp_df

def total_length(df, code):
    return len(df[df.code == code])

def get_good_list(df, max_length, code):
    return code if len(df[df.code == code]) == max_length else None  

if __name__ == '__main__':
    creview = CReivew(ct.DB_INFO)
    if not os.path.exists('norm.json'):
        if not os.path.exists('temp.json'):
            start_date = '2018-09-01'
            end_date   = '2018-09-10'
            #上证指数的涨跌数据
            szzs_df = CIndex(ct.DB_INFO, '000001').get_k_data_in_range(start_date, end_date)
            szzs_df['pchange'] = 100 * (szzs_df.close - szzs_df.open) / szzs_df.close
            szzs_df['preclose'] = szzs_df['close'].shift(-1)
            stock_info = CStockInfo.get()
            stock_info = stock_info[['code', 'name']]
            df = creview.gen_stocks_trends(start_date, end_date, stock_info)
            df = df.reset_index(drop = True)
            df.code = df.code.astype(str).str.zfill(6)
            df.close = df.close * df.adj
            df.open = df.open * df.adj
            df['pchange'] = 100 * (df.close - df.open) / df.close
            df['preclose'] = df['close'].shift(-1)
            with open('temp.json', 'w') as f:
                f.write(df.to_json(orient='records', lines=True))
        else:
            logger.info("begin read file")
            with open('temp.json', 'r') as f:
                df = pd.read_json(f.read(), orient='records', lines=True,  dtype = {'code' : str})

        logger.info("read file success")
        code_list = set(df.code.tolist())

        logger.info("start choose stock, length:%s" % len(code_list))
        obj_pool = Pool(300)
        MONEY_LIMIT = 100000000
        cfunc = partial(choose_stock, df)
        good_list = [code for code in obj_pool.imap_unordered(cfunc, code_list) if code is not None]

        logger.info("choose stock succeed, length:%s." % len(good_list))
        cfunc = partial(total_length, df)
        total_length_list = [code for code in obj_pool.imap_unordered(cfunc, good_list)]

        logger.info("begin compute length")
        max_length = np.argmax(np.bincount(total_length_list))

        logger.info("max length:%s" % max_length)
        cfunc = partial(get_good_list, df, max_length)
        good_list = [code for code in obj_pool.imap_unordered(cfunc, good_list) if code is not None]

        logger.info("get new data")
        df = df[df.code.isin(good_list)]
        df = df.reset_index(drop = True)
        df['name'] = ''
        df['industry'] = ''

        logger.info("set name and adjust pchange")
        cfunc = partial(adjust_name_and_pchange, df, stock_info, szzs_df.pchange.values)
            for tmp_df in obj_pool.imap_unordered(cfunc, good_list): df.at[tmp_df.index, ['name', 'pchange']] = tmp_df.values

        logger.info("normalize data")
        date_only_array = df.cdate.tolist()
        cfunc = partial(data_std, df)
        for tmp_df in obj_pool.imap_unordered(cfunc, date_only_array): df.at[tmp_df.index, 'pchange'] = tmp_df.values
        with open('norm.json', 'w') as f:
            f.write(df.to_json(orient='records', lines=True))
        obj_pool.kill()
    else:
        with open('norm.json', 'r') as f:
            df = pd.read_json(f.read(), orient='records', lines=True,  dtype = {'code' : str})

    #logger.info("finish to index pchange")
    #rdf = pd.DataFrame(columns=["source", "target", "C0", "C1", "B1", "B5", "B10"])
    #for s_code in good_list:
    #    s_df = df.loc[df.code == s_code]
    #    s_df = s_df.reset_index(drop = True)
    #    for t_code in good_list:
    #        t_df = df.loc[df.code == t_code]
    #        t_df = t_df.reset_index(drop = True)
    #        if s_code != t_code:
    #            tmp_df = pd.DataFrame()
    #            tmp_df[s_code] = s_df.pchange
    #            tmp_df[t_code] = t_df.pchange
    #            # calculate optimal hedge ratio "beta"
    #            model = sm.OLS(tmp_df[s_code], tmp_df[t_code])
    #            results = model.fit()
    #            series = results.params.tolist()
    #            # calculate the residuals of the linear combination
    #            tmp_df["res"] = tmp_df[s_code] - series * tmp_df[t_code]
    #            # calculate and output the CADF test on the residuals
    #            cadf = ts.adfuller(tmp_df["res"])
    #            if cadf[0] < cadf[4]['1%'] and cadf[1] < 0.00000001:
    #                print("source_code:%s, target_code:%s, C0:%s, C1:%s, B1:%s, B5:%s, B10:%s" % (s_code, t_code, cadf[0], cadf[1], cadf[4]['1%'], cadf[4]['5%'], cadf[4]['10%']))
    #                rdf.append({"source":s_code, "target":t_code, "C0":cadf[0], "C1":cadf[1], "B1":cadf[4]['1%'], "B5":cadf[4]['5%'], "B10":cadf[4]['10%']}, ignore_index=True)
    #                creview.plot_price_series(df, s_code, t_code)
    #logger.info("finish all")

    logger.info("finish to index pchange")
    for code in good_list:
        tmp_df = df.loc[df.code == code]
        series = tmp_df.close.tolist()
        #cadf = ts.adfuller(series)
        #if cadf[0] < cadf[4]['1%'] and cadf[1] < 0.00000001
        H, c, data = compute_Hc(series, kind='price', simplified=True, min_window = 5)
        print("code={:s}, H={:.4f}, c={:.4f}, data={}, cadf={}".format(code, H, c, data, cadf))
        #uncomment the following to make a plot using Matplotlib:
        #import matplotlib.pyplot as plt
        #f, ax = plt.subplots()
        #ax.plot(data[0], c*data[0]**H, color="deepskyblue")
        #ax.scatter(data[0], data[1], color="purple")
        #ax.set_xscale('log')
        #ax.set_yscale('log')
        #ax.set_xlabel('Time interval')
        #ax.set_ylabel('R/S ratio')
        #ax.grid(True)
        #plt.savefig('/tmp/relation.png', dpi=1000)
