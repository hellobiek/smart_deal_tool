# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import initializer
import const as ct
import numpy as np
import pandas as pd
from hgt import HGT
from rzrq import RZRQ
from tornado import gen
from climit import CLimit
from cindex import CIndex
from cstock import CStock
from bokeh.io import curdoc
from threading import Thread
from rstock import RIndexStock
from base.clog import getLogger
from bokeh.plotting import figure
from base.cdate import str_to_datetime
from scipy.stats import percentileofscore
from datamanager.investor import CInvestor
from bokeh.models.tools import CustomJSHover
from datetime import date, datetime, timedelta
from bokeh.layouts import row, column, grid, gridplot
from datamanager.sexchange import StockExchange
from algotrade.technical.ma import ma, ewma, macd
from bokeh.transform import linear_cmap, transform
from bokeh.palettes import Spectral11, Category20_13
from datamanager.bull_stock_ratio import BullStockRatio
from bokeh.models.widgets import AutocompleteInput, DatePicker
from bokeh.models import BoxZoomTool, Select, HoverTool, ColumnDataSource, Div, LinearColorMapper, ColorBar, ResetTool
class DashBoard(object):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        self.dbinfo = dbinfo
        self.hgt_client = HGT()
        self.rzrq_client = RZRQ(dbinfo)
        self.logger = getLogger(__name__)

def update_index(attr, old, new):
    code = index_select.value
    dtype = index_type_select.value
    start_date = dt_pckr_start.value
    end_date = dt_pckr_end.value
    print("index:", code, dtype, start_date, end_date)
    index_layout.children[1] = create_index_figure_column(code, dtype, start_date, end_date)

def create_index_figure_column(code, dtype, start_date, end_date):
    delta_days = (end_date - start_date).days
    if delta_days <= 0: return None
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    if dtype == 'K线图': 
        obj = CIndex(code)
        df = obj.get_k_data_in_range(start_date, end_date)
        df['date'] = df['date'].apply(lambda x:str_to_datetime(x, dformat = "%Y-%m-%d"))
        df = ma(df, 5)
        df = ma(df, 10)
        df = ma(df, 20)
        df = macd(df)
        source = ColumnDataSource(df)
        mapper = linear_cmap(field_name='pchange', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
        p = figure(plot_height=500, plot_width=1200, tools="", toolbar_location=None, sizing_mode="scale_both", x_range=(0, len(df)))
        p.xaxis.axis_label = "时间"
        p.yaxis.axis_label = "点数"

        p.segment(x0='index', y0='low', x1='index', y1='high', line_width=2, color='black', source=source)
        p.vbar(x='index', bottom='open', top='close', width = 50 / delta_days, color=mapper, source=source)
        p.xaxis.major_label_overrides = {i: mdate.strftime('%Y-%m-%d') for i, mdate in enumerate(df["date"])}

        volume_p = figure(plot_height=150, plot_width=1200, tools="", toolbar_location=None, sizing_mode="scale_both")
        volume_p.x_range = p.x_range
        volume_p.vbar(x='index', top='volume', width = 50 / delta_days, color=mapper, source=source)
        volume_p.xaxis.major_label_overrides = {i: mdate.strftime('%Y-%m-%d') for i, mdate in enumerate(df["date"])}
        return column(p, volume_p)
    else:
        df = BullStockRatio(code).get_k_data_between(start_date, end_date)
        cdates_list = df.index.tolist()
        value_list = df['ratio'].tolist()
        data = {
            'date': cdates_list,
            'value': value_list
        }
        source = ColumnDataSource(data)
        p = figure(plot_height=500, plot_width=1200, tools="", toolbar_location=None, sizing_mode="scale_both")
        p.xaxis.axis_label = "时间"
        p.yaxis.axis_label = "比例"
        p.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(df["date"])}
        p.line(x = 'date', y = 'value', line_width=3, line_alpha=1.0, source=source)
        p.add_tools(HoverTool(tooltips=[("数量", "@value")]))
        return column(p)

def create_stock_figure_column(code, start_date, end_date):
    obj = CStock(code)
    delta_days = (end_date - start_date).days
    if delta_days <= 0: return None
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    df = obj.get_k_data_in_range(start_date, end_date)
    if df is None: return None
    df['date'] = df['date'].apply(lambda x:str_to_datetime(x, dformat = "%Y-%m-%d"))
    df = ma(df, 5)
    df = ma(df, 10)
    df = ma(df, 20)
    df = macd(df)
    source = ColumnDataSource(df)
    mapper = linear_cmap(field_name='pchange', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
    p = figure(plot_height=500, plot_width=1300, tools="", toolbar_location=None, sizing_mode="scale_both", x_range=(0, len(df)))
    p.xaxis.axis_label = "时间"
    p.yaxis.axis_label = "点数"

    p.segment(x0='index', y0='low', x1='index', y1='high', line_width=2, color='black', source=source)
    p.vbar(x='index', bottom='open', top='close', width = 50 / delta_days, color=mapper, source=source)
    p.xaxis.major_label_overrides = {i: mdate.strftime('%Y-%m-%d') for i, mdate in enumerate(df["date"])}

    volume_p = figure(plot_height=150, plot_width=1300, tools="", toolbar_location=None, sizing_mode="scale_both")
    volume_p.x_range = p.x_range
    volume_p.vbar(x='index', top='volume', width = 50 / delta_days, color=mapper, source=source)
    volume_p.xaxis.major_label_overrides = {i: mdate.strftime('%Y-%m-%d') for i, mdate in enumerate(df["date"])}
    return column(p, volume_p)

def update_stock(attr, old, new):
    code = stock_auto_input.value
    start_date = stock_pckr_start.value
    end_date = stock_pckr_end.value
    print("code:", code, start_date, end_date)
    stock_layout.children[1] = create_stock_figure_column(code, start_date, end_date)

def create_hgt_figure(sh_df, sz_df):
    if sh_df is None or sz_df is None: return None
    y_dict = dict()
    y_dict['cum_buy'] = ((sh_df['cum_buy'] + sz_df['cum_buy'])).round(2).tolist()
    yval_max = max(y_dict['cum_buy'])
    y_dict['date'] = sh_df.date.tolist()
    y_dict['index'] = sh_df.index.tolist()
    data = {
        'index': sh_df.index.tolist(),
        'y': (10 * (sh_df['net_buy'] + sz_df['net_buy'])).round(2).tolist()
    }
    source = ColumnDataSource(data)
    p = figure(tools="", toolbar_location=None, x_range=(0, len(y_dict['date'])), y_range=(-yval_max * 0.2, yval_max * 1.3))

    mline = p.line(x = y_dict['index'], y = y_dict['cum_buy'], line_width=2, color=Spectral11[0], alpha=0.8, legend="沪港通买入累计余额")
    mapper = linear_cmap(field_name='y', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
    p.vbar(x='index', bottom=0, top='y', color=mapper, width=1, legend='融资变化', source = data)

    p.add_tools(HoverTool(tooltips=[("value", "@y")]))
    p.yaxis.axis_label = "沪港通数据概况"
    p.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(y_dict['date'])}

    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    p.legend.orientation = "horizontal"
    return p

def create_rzrq_figure(sh_df, sz_df):
    if sh_df is None or sz_df is None: return None
    y_dict = dict()
    y_dict['rzrqye'] = ((sh_df['rzrqye'] + sz_df['rzrqye'])).round(2).tolist()
    yval_max = max(y_dict['rzrqye'])
    y_dict['date'] = sh_df.date.tolist()
    y_dict['index'] = sh_df.index.tolist()
    data = {
        'index': sh_df.index.tolist(),
        'y': (10*((sh_df['rzmre'] + sz_df['rzmre']) - (sh_df['rzche'] + sz_df['rzche']))).round(2).tolist()
    }
    source = ColumnDataSource(data)
    p = figure(tools="", toolbar_location=None, x_range=(0, len(y_dict['date'])), y_range=(-yval_max * 0.2, yval_max * 1.3))

    mline = p.line(x = y_dict['index'], y = y_dict['rzrqye'], line_width=2.5, color=Spectral11[0], alpha=0.8, legend="融资融券余额")
    mapper = linear_cmap(field_name='y', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
    p.vbar(x='index', bottom=0, top='y', color=mapper, width=1, legend='融资变化', source = data)

    p.add_tools(HoverTool(tooltips=[("value", "@y")]))
    p.yaxis.axis_label = "融资融券概况"
    p.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(y_dict['date'])}

    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    p.legend.orientation = "horizontal"
    return p

def create_market_figure(sh_df, sz_df, ycolumn):
    if sh_df is None or sz_df is None: return None
    y_dict = dict()
    x_list = sh_df.date.tolist()
    y_dict['上海市场']  = sh_df[ycolumn].tolist()
    y_dict['深圳市场']  = sz_df[ycolumn].tolist()
    if ycolumn in ['pe', 'turnover']:
        y_dict['整体市场']  = ((sh_df[ycolumn] + sz_df[ycolumn])/2).round(2).tolist()
    else:
        y_dict['整体市场']  = (sh_df[ycolumn] + sz_df[ycolumn]).round(2).tolist()
    mypalette = Spectral11[0:len(y_dict)]
    yval_max = max(max(y_dict['整体市场']), max(y_dict['深圳市场']), max(y_dict['上海市场']))
    p = figure(plot_height=400, plot_width=600, tools="", toolbar_location=None, sizing_mode="scale_both", x_range=(0, len(x_list)), y_range=(0, yval_max * 1.3))
    y_dict['index'] = sh_df.index.tolist()
    y_dict['date'] = sh_df.date.tolist()
    p.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(y_dict['date'])}
    p.yaxis.axis_label = ycolumn
    line_list = list()
    for name, color in zip(['上海市场', '深圳市场', '整体市场'], mypalette):
        mline = p.line(x = y_dict['index'], y = y_dict[name], line_width=2, color=color, alpha=0.8, legend=name)
        line_list.append(mline)
    p.add_tools(HoverTool(tooltips=[("value", "@y")], renderers=line_list))
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    p.legend.orientation = "horizontal"
    return p

def generate_market_column(start_date, end_date):
    sh_df = get_market_data(ct.SH_MARKET_SYMBOL, start_date, end_date)
    sz_df = get_market_data(ct.SZ_MARKET_SYMBOL, start_date, end_date)
    date_list = list(set(sh_df.date.tolist()).intersection(set(sz_df.date.tolist())))
    sh_df = sh_df[sh_df.date.isin(date_list)]
    sh_df = sh_df.reset_index(drop = True)
    sz_df = sz_df[sz_df.date.isin(date_list)]
    sz_df = sz_df.reset_index(drop = True)
    pe_fig = create_market_figure(sh_df, sz_df, 'pe') 
    amount_fig = create_market_figure(sh_df, sz_df, 'amount') 
    turnover_fig = create_market_figure(sh_df, sz_df, 'turnover')
    negotiable_fig = create_market_figure(sh_df, sz_df, 'negotiable_value') 
    return gridplot([[amount_fig, turnover_fig], [negotiable_fig, pe_fig]])

def create_market_figure_column(start_date, end_date):
    delta_days = (end_date - start_date).days
    if delta_days <= 0: return None
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return generate_market_column(start_date, end_date)

def update_market(attr, old, new):
    start_date = market_pckr_start.value
    end_date = market_pckr_end.value
    market_layout.children[1] = create_market_figure_column(start_date, end_date)

def create_capital_figure_row():
    rzrq_fig = create_rzrq_figure(dboard.rzrq_client.sh_df, dboard.rzrq_client.sz_df)
    hgt_fig = create_hgt_figure(dboard.hgt_client.sh_df, dboard.hgt_client.sz_df)
    if rzrq_fig is None or hgt_fig is None:
        return row(figure(), figure())
    return row(children = [rzrq_fig, hgt_fig])

@gen.coroutine
def update_capital():
    capital_layout.children[1] = create_capital_figure_row()

def create_stats_figure(mdate):
    limit_info = CLimit().get_data(mdate)
    stock_info = RIndexStock().get_data(mdate)
    stock_info = stock_info[stock_info.volume > 0] #get volume > 0 stock list
    stock_info = stock_info.reset_index(drop = True)
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
    num_list.reverse()
    name_list.reverse()
    source = ColumnDataSource(data=dict(names = name_list, values = num_list, colors = Category20_13))
    p = figure(x_range = name_list, y_range=(0, max(num_list) + 100), title="涨跌幅统计")
    p.vbar(x='names', top='values', width=0.9, color='colors', source=source)
    p.xgrid.grid_line_color = None
    p.add_tools(HoverTool(tooltips=[("涨跌幅", "@names"), ("数量", "@values")]))
    return p

def create_marauder_map_figure(mdate):
    df = RIndexStock().get_data(mdate)
    TOOLTIPS = [("code", "@code"), ("(pday, profit)", "(@pday, @profit)")]
    TOOLS = [BoxZoomTool(), ResetTool(), HoverTool(tooltips = TOOLTIPS)]
    p = figure(x_axis_label='时间', y_axis_label='强度', tools=TOOLS, toolbar_location="above", title="牛熊比例")
    if df is None or df.empty: return p
    source = ColumnDataSource(df)
    color_mapper = LinearColorMapper(palette="Viridis256", low=df.profit.min(), high=df.profit.max())
    p.circle(x='pday', y='profit', color=transform('profit', color_mapper), size=5, alpha=0.6, source=source)
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=12, location=(0,0), title='强度')
    p.add_layout(color_bar, 'right')
    return p

def generate_technique_row(mdate):
    stats_fig = create_stats_figure(mdate)
    marauder_fig = create_marauder_map_figure(mdate)
    return row(stats_fig, marauder_fig)

def update_technique(attr, old, new):
    mdate = technique_pckr.value
    technique_layout.children[1] = create_technique_figure_row(mdate)

def create_technique_figure_row(mdate):
    mdate = mdate.strftime('%Y-%m-%d')
    return generate_technique_row(mdate)

def get_market_data(market, start_date, end_date):
    obj = StockExchange(market)
    if market == ct.SH_MARKET_SYMBOL:
        df = obj.get_k_data_in_range(start_date, end_date)
        df = df.loc[df.name == '上海市场']
    else:
        df = obj.get_k_data_in_range(start_date, end_date)
        df = df.loc[df.name == '深圳市场']
    df = df.round(2)
    df = df.drop_duplicates()
    df = df.sort_values(by = 'date', ascending= True)
    df.negotiable_value = (df.negotiable_value / 2).astype(int)
    df = df.reset_index(drop = True)
    return df

def update_valuation(attr, old, new):
    start_date = valuation_pckr_start.value
    end_date = valuation_pckr_end.value
    valuation_layout.children[1] = create_valuation_figure_column(start_date, end_date)

def get_valuation_data(code_dict, start_date, end_date):
    data_dict = dict()
    for code, name in code_dict.items():
        df = CIndex(code).get_val_data()
        df = df.loc[(df.date >= start_date) & (df.date <= end_date)]
        df = df.sort_values(by=['date'], ascending = True)
        df = df.reset_index(drop = True)
        data_dict[code] = df
    data_dict['date'] = df.date.tolist()
    return data_dict

def create_valuation_figure(data_dict, code_dict, dtype):
    line_list = list()
    mypalette = Spectral11[0:len(code_dict)]
    p = figure(plot_height=400, plot_width=600, tools="", toolbar_location=None, sizing_mode="scale_both", x_range=(0, len(data_dict['date'])), y_range=(0, 130))
    for code, color in zip(code_dict.keys(), mypalette):
        name = code_dict[code]
        index_list = data_dict[code].index.tolist()
        value_list = data_dict[code][dtype].tolist()
        sorted_value_list = sorted(value_list)
        percentile_list = data_dict[code][dtype].apply(lambda x: percentileofscore(sorted_value_list, x))
        mline = p.line(x = index_list, y = percentile_list, line_width=2, color=color, alpha=0.8, legend=name)
        line_list.append(mline)
    p.legend.click_policy="hide"
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.xaxis.axis_label = "时间"
    p.yaxis.axis_label = dtype
    p.add_tools(HoverTool(tooltips=[("value", "@y")], renderers=line_list))
    p.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(data_dict["date"])}
    return p

def generate_valuation_row(start_date, end_date):
    code_dict = {'000016':'上证50', '000300':'沪深300', '000905':'中证500', '399006':'创业板指'}
    df = get_valuation_data(code_dict, start_date, end_date)
    pe_fig = create_valuation_figure(df, code_dict, 'pe')
    pb_fig = create_valuation_figure(df, code_dict, 'pb')
    roe_fig = create_valuation_figure(df, code_dict, 'roe')
    dr_fig = create_valuation_figure(df, code_dict, 'dr')
    return gridplot([[pe_fig, pb_fig], [roe_fig, dr_fig]])

def create_valuation_figure_column(start_date, end_date):
    delta_days = (end_date - start_date).days
    if delta_days <= 0: return None
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return generate_valuation_row(start_date, end_date)

def generate_investors_fig(df, dtype):
    clegend = '个人' if dtype == 'nature' else '机构'
    ckey = 'final_natural_a_person' if dtype == 'nature' else 'final_non_natural_a_person'
    value_list = df[ckey].tolist()
    cdates_list = df['date'].tolist()
    cdates_str_list = df['date_str'].tolist()
    data = {
        'date': cdates_list,
        'value': value_list,
        'date_str': cdates_str_list
    }
    source = ColumnDataSource(data)
    p = figure(tools="", toolbar_location=None, x_axis_type='datetime')
    p.xaxis.axis_label = "时间"
    p.yaxis.axis_label = "数量"
    p.line(x = 'date', y = 'value', line_width = 2, line_alpha = 1.0, source = source)
    p.add_tools(HoverTool(tooltips=[("日期", "@date_str"), ("数量", "@value")]))
    return p

def create_investors_row(start_date, end_date):
    delta_days = (end_date - start_date).days
    if delta_days <= 0: return None
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    df = CInvestor().get_data_in_range(start_date, end_date)
    df = df.loc[df.final_investor > 0]
    df['date_str'] = df['date']
    df['date'] = df['date'].apply(lambda x:str_to_datetime(x, dformat = "%Y-%m"))
    df = df.reset_index(drop = True)
    nature_fig = generate_investors_fig(df, 'nature')
    unnature_fig = generate_investors_fig(df, 'unnature')
    return row(nature_fig, unnature_fig)

def update_investors(attr, old, new):
    start_date = investors_pckr_start.value
    end_date = investors_pckr_end.value
    investors_layout.children[1] = create_investors_row(start_date, end_date)

# Overview Data
def get_overview_data():
    cdate = (datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')
    code_dict = {
        '000001': '上证指数',
        '399001': '深证成指',
        '399006': '创业板指',
        '000016': '上证50',
        '000905': '中证500'
    }
    overview_dict = {
        '上证指数' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '上证指数', 'cdate': cdate},
        '深证成指' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '深证成指', 'cdate': cdate},
        '创业板指' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '创业板指', 'cdate': cdate},
        '上证50' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '上证50', 'cdate': cdate},
        '中证500' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '中证500', 'cdate': cdate},
        '科创板指' : {'icon': 'dollar-sign', 'value': 0, 'change': 0, 'label': '科创板指', 'cdate': cdate}
    }
    for code, name in code_dict.items():
        df = CIndex(code).get_k_data(cdate)
        if df is None: return overview_dict
        if df.empty:
            overview_dict[name]['value'] = 0
            overview_dict[name]['change'] = 0
        else:
            row = df.to_dict('records')[0]
            overview_dict[name]['value'] = round(row['close'], 2)
            overview_dict[name]['change'] = round(row['pchange'], 2)
    return overview_dict

cdoc = curdoc()
dboard = DashBoard()

# IndexSelecter
index_codes = list(ct.INDEX_DICT.keys())
index_codes.append('880883')
index_type_list = ['K线图', '牛熊股比']
index_select = Select(value='000001', title='指数代码', options=sorted(index_codes), height=50)
index_type_select = Select(value='牛熊股比', title='数据类型', options=sorted(index_type_list), height=50)
# DatePciekr
dt_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 200), min_date = date(2004,1,1), max_date = date.today())
dt_pckr_end = DatePicker(title='结束日期', value = date.today(), min_date = date(2004,1,1), max_date = date.today())

index_select_row = row(index_select, index_type_select, dt_pckr_start, dt_pckr_end)
index_layout = column(index_select_row, create_index_figure_column(index_select.value, index_type_select.value, dt_pckr_start.value, dt_pckr_end.value))

index_select.on_change('value', update_index)
index_type_select.on_change('value', update_index)
dt_pckr_start.on_change('value', update_index)
dt_pckr_end.on_change('value', update_index)

# Add Stock Analysis
stock_auto_input = AutocompleteInput(value = '601318', completions = initializer.update_code_list(), title = '股票代码')
# DatePciekr
stock_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 100), min_date = date(2000,1,1), max_date = date.today())
stock_pckr_end = DatePicker(title='股票日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())

stock_select_row = row(stock_auto_input, stock_pckr_start, stock_pckr_end)
stock_layout = column(stock_select_row, create_stock_figure_column(stock_auto_input.value, stock_pckr_start.value, stock_pckr_end.value))

stock_auto_input.on_change('value', update_stock)
stock_pckr_start.on_change('value', update_stock)
stock_pckr_end.on_change('value', update_stock)

# Market Data
# DatePciekr
market_title = Div(text="整体市场概况", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
market_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 100), min_date = date(2000,1,1), max_date = date.today())
market_pckr_end = DatePicker(title='股票日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
market_select_row = row(market_title, market_pckr_start, market_pckr_end)
market_layout = column(market_select_row, create_market_figure_column(market_pckr_start.value, market_pckr_end.value))

market_pckr_start.on_change('value', update_market)
market_pckr_end.on_change('value', update_market)

# Capital Data
# DatePciekr
capital_title = Div(text="资金面概况", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
capital_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 200), min_date = date(2000,1,1), max_date = date.today())
capital_pckr_end = DatePicker(title='股票日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
capital_select_row = row(capital_title, capital_pckr_start, capital_pckr_end)
capital_layout = column(capital_select_row, create_capital_figure_row())

#capital_pckr_start.on_change('value', update_capital)
#capital_pckr_end.on_change('value', update_capital)

# Techinque Data
technique_title = Div(text="技术面概况", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
technique_pckr = DatePicker(title='开始日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())

technique_select_row = row(technique_title, technique_pckr)
technique_layout = column(technique_select_row, create_technique_figure_row(technique_pckr.value))
technique_pckr.on_change('value', update_technique)

# Valuation Data
valuation_title = Div(text="基本面概况", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
valuation_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 100), min_date = date(2000,1,1), max_date = date.today())
valuation_pckr_end = DatePicker(title='股票日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
valuation_select_row = row(valuation_title, valuation_pckr_start, valuation_pckr_end)
valuation_layout = column(valuation_select_row, create_valuation_figure_column(valuation_pckr_start.value, valuation_pckr_end.value))
valuation_pckr_start.on_change('value', update_valuation)
valuation_pckr_end.on_change('value', update_valuation)

# Data Investors
investors_title = Div(text="投资者概况", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
investors_pckr_start = DatePicker(title='开始日期', value = date.today() - timedelta(days = 3650), min_date = date(2000,1,1), max_date = date.today())
investors_pckr_end = DatePicker(title='股票日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
investors_select_row = row(investors_title, investors_pckr_start, investors_pckr_end)
investors_layout = column(investors_select_row, create_investors_row(investors_pckr_start.value, investors_pckr_end.value))
investors_pckr_start.on_change('value', update_investors)
investors_pckr_end.on_change('value', update_investors)

layout = column(index_layout, investors_layout, market_layout, 
                capital_layout, valuation_layout, technique_layout, 
                stock_layout, sizing_mode="scale_both", name="layout")

def blocking_task():
    while True:
        start_date = capital_pckr_start.value
        end_date = capital_pckr_end.value
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        dboard.rzrq_client.update(start_date, end_date)
        dboard.hgt_client.update(start_date, end_date)
        cdoc.add_next_tick_callback(update_capital)

thread = Thread(target=blocking_task)
thread.setDaemon(True)
thread.start()

overview_dict = get_overview_data()

cdoc.title = "万花筒"
cdoc.add_root(layout)
cdoc.template_variables['stats_names'] = overview_dict.keys()
cdoc.template_variables['stats'] = overview_dict
