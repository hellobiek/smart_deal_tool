# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import numpy as np
import pandas as pd
from tornado import gen
from functools import partial
from cstock_info import CStockInfo
from cpython.cval import CValuation, DTYPE_DICT
from datetime import date, timedelta
from bokeh.layouts import row, column
from base.cdate import datetime_to_int
from bokeh.plotting import curdoc, figure
from bokeh.document import without_document_lock
from concurrent.futures import ThreadPoolExecutor
from bokeh.models import ColumnDataSource, Select, DatePicker, HoverTool
def update(industry, dtype, mdate):
    if dtype == 'industry': return stock_df
    dtype_list = [dtype]
    if industry == '所有':
        df = stock_df
        print(mdate, dtype_list)
        if dtype == '质押率':
            pdf = cvaluation.get_stock_pledge_info(df, dtype_list, mdate)
            vdf = pdf.loc[pdf.code.isin(df.code.tolist())]
            df = vdf.reset_index(drop = True)
        else:
            cvaluation.update_vertical_data(df, dtype_list, mdate)
    else:
        df = stock_df.loc[stock_df.industry == industry]
        df = df.reset_index(drop = True)
        print(mdate, dtype_list, industry)
        cvaluation.update_vertical_data(df, dtype_list, mdate)
    df = df.dropna(subset = dtype_list)
    df = df.reset_index(drop = True)
    return df

@gen.coroutine
def locked_update(data, dtype):
    bin_num = 50
    values = data[dtype].tolist()
    hist, edges = np.histogram(values, density=False, bins=bin_num)
    percentile_list = hist.cumsum() / hist.sum()
    vsource.data = dict(bottom = [0 for n in range(bin_num)], top = hist, left = edges[0:len(edges)-1], right = edges[1:len(edges)], percentile = percentile_list)

@gen.coroutine
@without_document_lock
def unlocked_task():
    mdate = date_picker.value
    dtype = value_select.value
    industry = industry_select.value
    mdate = datetime_to_int(mdate)
    vdata = yield executor.submit(update, industry, dtype, mdate)
    if dtype != 'industry': vdata = vdata[vdata[dtype] > -30]
    cdoc.add_next_tick_callback(partial(locked_update, data=vdata, dtype = dtype))

#initialize figure
def make_plot():
    fig = figure(plot_width=1200, plot_height=600,tools="pan,box_zoom,wheel_zoom,reset,box_select,lasso_select", toolbar_location="right", background_fill_color="#fafafa")
    fig.y_range.start = 0
    fig.add_tools(HoverTool(tooltips=[("数量", "@top"), ("数值", "@left"), ("分位数", "@percentile")]))
    fig.xaxis.axis_label = 'x'
    fig.yaxis.axis_label = 'Vol(x)'
    fig.grid.grid_line_color="white"
    return fig

cdoc = curdoc()
cvaluation = CValuation()
stock_info_client = CStockInfo()
vsource = ColumnDataSource(dict(bottom = list(), top = list(), left = list(), right = list(), percentile = list()))

fig = make_plot()
fig.quad(top='top', bottom='bottom', left='left', right='right', fill_color="navy", line_color="white", alpha=0.5, source = vsource)

stock_df = stock_info_client.get()
industries = list(set(stock_df.industry.tolist()))
industries.append("所有")

date_picker = DatePicker(title='日期', value = date.today() - timedelta(days = 200), min_date = date(2004,1,1), max_date = date.today())
industry_select = Select(title='行业', value='所有', options=sorted(industries), height=50)
cols = list(DTYPE_DICT.keys())
cols.append('industry')
value_select = Select(title='类型', value='roa', options=sorted(cols), height=50)

controls = row(industry_select, value_select, date_picker)
layout = column(controls, fig)

executor = ThreadPoolExecutor(max_workers=2)

cdoc.add_root(layout)
cdoc.add_periodic_callback(unlocked_task, 10000)
cdoc.title = "指标分布"
