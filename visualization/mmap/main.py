# -*- coding: utf-8 -*-
import os
import sys
import traceback
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
from cstock import CStock
from datetime import date
from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.events import DoubleTap
from cpython.cval import CValuation
from base.cdate import int_to_datetime
from bokeh.layouts import row, column, gridplot
from bokeh.transform import linear_cmap, transform
from visualization.marauder_map import MarauderMap
from bokeh.models.widgets import DatePicker, DataTable, TableColumn, TextInput
from bokeh.models import WheelZoomTool, BoxZoomTool, HoverTool, ColumnDataSource, Div, LinearColorMapper, ColorBar, ResetTool, CustomJS, LassoSelectTool, BoxSelectTool, PanTool, TapTool
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
    df = mmap.get_data(mdate)
    TOOLTIPS = [("code", "@code"), ("(pday, profit)", "(@pday, @profit)")]
    TOOLS = [TapTool(), PanTool(), BoxZoomTool(), WheelZoomTool(), ResetTool(), BoxSelectTool(), HoverTool(tooltips = TOOLTIPS)]
    p = figure(plot_height=800, plot_width=1300, x_axis_label='时间', y_axis_label='强度', tools=TOOLS, toolbar_location="above", title="活点地图")
    if df is None or df.empty: return p
    mdict = {'code': df.code.tolist(), 'profit': df.profit.tolist(), 'pday': df.pday.tolist()}
    global msource
    msource = ColumnDataSource(mdict)
    color_mapper = LinearColorMapper(palette = "Viridis256", low = min(mdict['profit']), high = max(mdict['profit']))
    p.circle(x = 'pday', y = 'profit', color = transform('profit', color_mapper), size = 5, alpha = 0.6, source = msource)
    color_bar = ColorBar(color_mapper = color_mapper, label_standoff = 12, location = (0,0), title = '强度')
    p.add_layout(color_bar, 'right')
    callback = CustomJS(args=dict(msource = msource, tsource = tsource, mtable = mtable), code="""
            var inds = cb_obj.indices;
            var d1 = msource.data;
            var d2 = tsource.data;
            d2['code'] = []
            d2['pday'] = []
            d2['profit'] = []
            for (var i = 0; i < inds.length; i++) {
                d2['code'].push(d1['code'][inds[i]])
                d2['profit'].push(d1['profit'][inds[i]])
                d2['pday'].push(d1['pday'][inds[i]])
            }
            tsource.change.emit();
            mtable.change.emit()
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

def create_roe_figure(val_source):
    TOOLTIPS = [("roa", "@roa")]
    TOOLS = [HoverTool(tooltips = TOOLTIPS)]
    fig = figure(plot_height = profit_fig.plot_height, plot_width = dist_fig.plot_width,
                            x_axis_type='datetime', tools=TOOLS, toolbar_location=None)
    fig.vbar(x = 'date', bottom = 0, top = 'roa', width = 50, color = 'blue', source = val_source)
    return fig

cdoc = curdoc()
mmap = MarauderMap()
cdoc.title = "活点地图"
code_text = TextInput(value = None, title = "代码:", width = 420)
code_text.on_change('value', update_stock)
msource = ColumnDataSource(dict(code = list(), pday = list(), profit = list()))
mmap_title = Div(text="股票分析", width=120, height=40, margin=[25, 0, 0, 0], style={'font-size': '150%', 'color': 'blue'})
mmap_pckr = DatePicker(title='开始日期', value = date.today(), min_date = date(2000,1,1), max_date = date.today())
mmap_pckr.on_change('value', update_mmap)
mmap_select_row = row(mmap_title, mmap_pckr)
tsource = ColumnDataSource(dict(code = list(), pday = list(), profit = list()))
source_code = """
    row = cb_obj.indices[0]
    text_row.value = String(source.data['code'][row]);
"""
callback = CustomJS(args = dict(source = tsource, text_row = code_text), code = source_code)
tsource.selected.js_on_change('indices', callback)
columns = [TableColumn(field = "code", title = "代码"), TableColumn(field = "pday", title = "牛熊天数", sortable = True), TableColumn(field = "profit",title = "牛熊程度", sortable = True)]
mtable = DataTable(source = tsource, columns = columns, width = 1300, height = 200)

val_client = CValuation()

roe_fig = figure()
dist_fig = figure()
stock_fig = figure()
profit_fig = figure()

val_source = ColumnDataSource()
dist_source = ColumnDataSource()
stock_source = ColumnDataSource()

#code = '600900'
#mdate = '2019-08-08'
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

layout = column(mmap_select_row, create_mmap_figure_row(mmap_pckr.value), mtable, code_text, stock_row, name = "layout")
cdoc.add_root(layout)
