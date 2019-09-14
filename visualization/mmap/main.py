# -*- coding: utf-8 -*-
import os
import sys
import traceback
from os.path import abspath, dirname, join
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import itertools
import const as ct
import pandas as pd
from cstock import CStock
from datetime import date
from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.events import DoubleTap
from cstock_info import CStockInfo
from cpython.cval import CValuation
from base.cdate import int_to_datetime
from bokeh.palettes import Dark2_5 as palette
from bokeh.layouts import row, column, gridplot
from visualization.marauder_map import MarauderMap
from bokeh.transform import linear_cmap, transform
from bokeh.models.widgets import DatePicker, DataTable, TableColumn, TextInput, Button
from crawler.dspider.china_security_industry_valuation import ChinaSecurityIndustryValuationCrawler
from crawler.dspider.security_exchange_commission_valuation import SecurityExchangeCommissionValuationCrawler
from bokeh.models import WheelZoomTool, BoxZoomTool, HoverTool, ColumnDataSource, Div, LinearColorMapper, ColorBar, ResetTool, CustomJS, BoxSelectTool, PanTool, TapTool
def update_mmap(attr, old, new):
    mdate = mmap_pckr.value
    layout.children[1] = create_mmap_figure_row(mdate)

#add a dot where the click happened
def scallback(event):
    global dist_source, dist_fig, roe_fig, profit_fig
    code = code_text.value
    sobj = CStock(code)
    mdate = stock_source.data['date'][int(event.x)]
    print(code, mdate)
    ddf = sobj.get_chip_distribution(mdate)
    dist_source = ColumnDataSource(ddf)
    dist_fig = create_dist_figure(dist_source)
    layout.children[4] = gridplot([[stock_fig, dist_fig], [profit_fig, roe_fig]])

def create_mmap_figure(mdate):
    TOOLTIPS = [("code", "@code"), ("(pday, profit)", "(@pday, @profit)")]
    TOOLS = [TapTool(), PanTool(), BoxZoomTool(), WheelZoomTool(), ResetTool(), BoxSelectTool(), HoverTool(tooltips = TOOLTIPS)]
    p = figure(plot_height=800, plot_width=1300, x_axis_label='时间', y_axis_label='强度', tools=TOOLS, toolbar_location="above", title="活点地图")
    df = mmap.get_data(mdate)
    df = pd.merge(df, base_df, how='inner', on=['code'])
    df = df[(df['timeToMarket'] < 20141231) | df.code.isin(list(ct.WHITE_DICT.keys()))]
    csi_df = csi_client.get_k_data(mdate)
    if csi_df is None or csi_df.empty: return p
    csi_df = csi_df.drop('name', axis=1)
    df = pd.merge(csi_df, df, how='inner', on=['code'])
    if df is None or df.empty: return p
    mdict = {'code': df.code.tolist(), 'name': df.name.tolist(), 'profit': df.profit.tolist(), 'pday': df.pday.tolist(),
            'industry': df.industry.tolist(), 'tind_name': df.tind_name.tolist(), 'find_name': df.find_name.tolist()}
    global msource, isource
    msource = ColumnDataSource(mdict)
    color_mapper = LinearColorMapper(palette = "Viridis256", low = min(mdict['profit']), high = max(mdict['profit']))
    p.circle(x = 'pday', y = 'profit', color = transform('profit', color_mapper), size = 5, alpha = 0.6, source = msource)
    color_bar = ColorBar(color_mapper = color_mapper, label_standoff = 12, location = (0,0), title = '强度')
    p.add_layout(color_bar, 'right')
    callback = CustomJS(args=dict(msource = msource, tsource = tsource, mtable = mtable, isource = isource), code="""
            var inds = cb_obj.indices;
            var d1 = msource.data;
            var d2 = tsource.data;
            var d3 = isource.data;
            var ndata = {};
            var tind_industry = '';
            var find_industry = '';
            var tind_industrys = [];
            var find_industrys = [];
            d2['code'] = [];
            d2['name'] = [];
            d2['pday'] = [];
            d2['profit'] = [];
            d2['industry'] = [];
            d2['tind_name'] = [];
            d2['find_name'] = [];
            for (var i = 0; i < inds.length; i++) {
                d2['code'].push(d1['code'][inds[i]])
                d2['name'].push(d1['name'][inds[i]])
                d2['profit'].push(d1['profit'][inds[i]])
                d2['pday'].push(d1['pday'][inds[i]])
                d2['industry'].push(d1['industry'][inds[i]])
                d2['tind_name'].push(d1['tind_name'][inds[i]])
                d2['find_name'].push(d1['find_name'][inds[i]])
                tind_industry = d1['tind_name'][inds[i]];
                find_industry = d1['find_name'][inds[i]];
                if(!tind_industrys.includes(tind_industry)){
                    tind_industrys.push(tind_industry)
                }
                if(!find_industrys.includes(find_industry)){
                    find_industrys.push(find_industry)
                }
                if(!(tind_industry in ndata)){
                    ndata[tind_industry] = {}
                }
                if(!(find_industry in ndata[tind_industry])){
                    ndata[tind_industry][find_industry] = []
                }
                ndata[tind_industry][find_industry].push(d1['code'][inds[i]])
            }
            d3['tind_industrys'] = tind_industrys;
            var tLength = tind_industrys.length;
            var fLength = find_industrys.length;
            for (var i = 0; i < fLength; i++) {
                d3[find_industrys[i]] = []
                for(var j = 0; j < tLength; j++) {
                    d3[find_industrys[i]].push(0);
                }
            }
            for (var i = 0; i < tLength; i++) {
                tind_industry = tind_industrys[i]
                for (var j = 0; j < fLength; j++) {
                    find_industry = find_industrys[j]
                    if(find_industry in ndata[tind_industry]){
                        d3[find_industry][i] = ndata[tind_industry][find_industry].length
                    }
                }
            }
            tsource.data = d2;
            isource.data = d3;
            tsource.change.emit();
            isource.change.emit();
            mtable.change.emit();
        """)
    msource.selected.js_on_change('indices', callback)
    return p

def create_mmap_figure_row(mdate):
    mdate = mdate.strftime('%Y-%m-%d')
    return row(create_mmap_figure(mdate))

def get_val_data(code):
    vdf = val_client.get_horizontal_data(code)
    vdf = vdf[(vdf['date'] - 1231) % 10000 == 0]
    vdf = vdf[-5:]
    vdf = vdf.reset_index()
    vdf['date'] = vdf['date'].apply(lambda x:int_to_datetime(x))
    return vdf

def update_stock(attr, old, new):
    code = code_text.value
    if code is None: return
    sobj = CStock(code)
    sdf = sobj.get_k_data()
    if sdf is None: return
    vdf = get_val_data(code)
    global stock_fig, profit_fig, dist_fig, roe_fig, stock_source, dist_source, val_source
    mdate = mmap_pckr.value
    mdate = mdate.strftime('%Y-%m-%d')
    ddf = sobj.get_chip_distribution(mdate)
    stock_source = ColumnDataSource(sdf)
    dist_source = ColumnDataSource(ddf)
    val_source = ColumnDataSource(vdf)
    stock_fig = create_stock_figure(stock_source)
    profit_fig = create_profit_figure(stock_source)
    dist_fig = create_dist_figure(dist_source)
    roe_fig = create_roe_figure(val_source)
    stock_fig.on_event(DoubleTap, scallback)
    layout.children[4] = gridplot([[stock_fig, dist_fig], [profit_fig, roe_fig]])

def create_profit_figure(stock_source):
    mapper = linear_cmap(field_name='profit', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
    fig = figure(plot_height=200, plot_width=1000, x_range = (stock_source.data['index'].min(), stock_source.data['index'].max()),
                 y_range = (stock_source.data['profit'].min(), stock_source.data['profit'].max()))
    fig.segment(x0='index', y0=0, x1='index', y1='profit', line_width=1, color=mapper, alpha=0.5, source=stock_source)
    fig.xaxis.axis_label = None
    fig.yaxis.axis_label = None
    fig.xaxis.major_tick_line_color = None
    fig.xaxis.minor_tick_line_color = None
    fig.yaxis.major_tick_line_color = None
    fig.yaxis.minor_tick_line_color = None
    fig.xaxis.major_label_text_color = None
    fig.yaxis.major_label_text_color = None
    return fig

def create_stock_figure(stock_source):
    mapper = linear_cmap(field_name='pchange', palette=['red', 'green'], low=0, high=0, low_color = 'green', high_color = 'red')
    TOOLTIPS = [("open", "@open"), ("high", "high"), ("low", "@low"), ("close", "@close"), ("pchange", "@pchange"), ("date", "@date")]
    TOOLS = [TapTool(), PanTool(), BoxZoomTool(), WheelZoomTool(), ResetTool(), BoxSelectTool(), HoverTool(tooltips = TOOLTIPS)]
    fig = figure(plot_height=500, plot_width=1000, x_axis_label='时间', y_axis_label='价格', tools=TOOLS, toolbar_location="above",
                x_range = (stock_source.data['index'].min(), stock_source.data['index'].max()),
                y_range = (stock_source.data['low'].min(), stock_source.data['high'].max()))
    fig.line(x = 'index', y = 'uprice', color = 'blue', line_width = 2, source = stock_source, name = '无穷成本均线')
    fig.segment(x0 = 'index', y0 = 'low', x1 = 'index', y1 = 'high', line_width = 1.5, color = 'black', source = stock_source)
    fig.vbar(x = 'index', bottom = 'open', top = 'close', width = 1, color = mapper, source = stock_source)
    fig.xaxis.major_label_overrides = {i: mdate for i, mdate in enumerate(stock_source.data["date"])}
    return fig

def create_dist_figure(dist_source):
    fig = figure(plot_width = 300, plot_height = stock_fig.plot_height, y_range = (stock_source.data['low'].min(), stock_source.data['high'].max()))
    fig.segment(x0 = 0, y0 = 'price', x1 = 'volume', y1 = 'price', line_width = 1, color = 'black', source = dist_source)
    fig.xaxis.axis_label = None
    fig.yaxis.axis_label = None
    fig.xaxis.major_tick_line_color = None
    fig.xaxis.minor_tick_line_color = None
    fig.yaxis.major_tick_line_color = None
    fig.yaxis.minor_tick_line_color = None
    fig.xaxis.major_label_text_color = None
    fig.yaxis.major_label_text_color = None
    return fig

def create_industry_figure():
    tind_industrys = isource.data['tind_industrys']
    find_industrys = list(isource.data.keys())
    find_industrys.remove('tind_industrys')
    colors = list()
    for _, color in zip(range(len(find_industrys)), itertools.cycle(palette)):
        colors.append(color)

    fig = figure(x_range = tind_industrys, plot_width = 1400, plot_height = 600, title="行业分布", 
                            toolbar_location=None, tools="hover", tooltips="@tind_industrys $name: @$name")
    fig.vbar_stack(find_industrys, x = 'tind_industrys', width = 0.9, color = colors, source = isource)
    fig.y_range.start = 0
    fig.x_range.range_padding = 0.1
    fig.xaxis.major_label_orientation = "vertical"
    fig.xgrid.grid_line_color = None
    fig.axis.minor_tick_line_color = None
    fig.outline_line_color = None
    return fig

def create_roe_figure(val_source):
    TOOLTIPS = [("roa", "@roa")]
    TOOLS = [HoverTool(tooltips = TOOLTIPS)]
    fig = figure(plot_height = profit_fig.plot_height, plot_width = dist_fig.plot_width, x_axis_type='datetime', tools=TOOLS, toolbar_location=None)
    fig.vbar(x = 'date', bottom = 0, top = 'roa', width = 50, color = 'blue', source = val_source)
    return fig

cdoc = curdoc()
mmap = MarauderMap()
stock_info_client = CStockInfo()
base_df = stock_info_client.get()
cdoc.title = "活点地图"
code_text = TextInput(value = None, title = "代码:", width = 420)
code_text.on_change('value', update_stock)
msource = ColumnDataSource(dict(code = list(), pday = list(), profit = list()))
mmap_title = Div(text="股票分析", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
mmap_pckr = DatePicker(title='开始日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
mmap_pckr.on_change('value', update_mmap)
mmap_select_row = row(mmap_title, mmap_pckr)
tsource = ColumnDataSource(dict(code = list(), pday = list(), profit = list(), industry = list(), tind_name = list(), find_name = list()))
source_code = """
    row = cb_obj.indices[0]
    text_row.value = String(source.data['code'][row]);
"""
tcallback = CustomJS(args = dict(source = tsource, text_row = code_text), code = source_code)
tsource.selected.js_on_change('indices', tcallback)
columns = [TableColumn(field = "code", title = "代码"), TableColumn(field = "name", title = "名字"), TableColumn(field = "pday", title = "牛熊天数", sortable = True), 
           TableColumn(field = "profit",title = "牛熊程度", sortable = True), TableColumn(field = "industry", title = "通达信行业"),
           TableColumn(field = "tind_name", title = "主行业"), TableColumn(field = "find_name", title = "子行业")]
mtable = DataTable(source = tsource, columns = columns, width = 1300, height = 200)

button = Button(label="download", button_type="success")
button.callback = CustomJS(args=dict(source=tsource), code=open(join(dirname(__file__), "download.js")).read())

val_client = CValuation()
csi_client = ChinaSecurityIndustryValuationCrawler()
#sec_client = SecurityExchangeCommissionValuationCrawler()

isource = ColumnDataSource({'tind_industrys': list()})
ibutton = Button(label="行业分析", button_type="success")
def change_click():
    layout.children[3] = column(ibutton, create_industry_figure())
ibutton.on_click(change_click)

roe_fig = figure()
dist_fig = figure()
stock_fig = figure()
profit_fig = figure()
industry_fig = figure()

table_column = column(button, mtable)

industry_column = column(ibutton, industry_fig)

val_source = ColumnDataSource()
dist_source = ColumnDataSource()
stock_source = ColumnDataSource()

#code = '300760'
#mdate = '2019-09-12'
#sobj = CStock(code)
#sdf = sobj.get_k_data()
#ddf = sobj.get_chip_distribution(mdate)
#stock_source = ColumnDataSource(sdf)
#dist_source = ColumnDataSource(ddf)
#vdf = get_val_data(code)
#val_source = ColumnDataSource(vdf)
#stock_fig = create_stock_figure(stock_source)
#dist_fig = create_dist_figure(dist_source)
#profit_fig = create_profit_figure(stock_source)
#roe_fig = create_roe_figure(val_source)

stock_row = gridplot([[stock_fig, dist_fig], [profit_fig, roe_fig]])
layout = column(mmap_select_row, create_mmap_figure_row(mmap_pckr.value), table_column, industry_column, code_text, stock_row, name = "layout")
cdoc.add_root(layout)
