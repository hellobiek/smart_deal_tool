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
from cpython.cval import CValuation
from datetime import date, timedelta
from bokeh.layouts import row, column
from base.cdate import datetime_to_str
from bokeh.plotting import curdoc, figure
from bokeh.document import without_document_lock
from concurrent.futures import ThreadPoolExecutor
from bokeh.models import ColumnDataSource, Select, DatePicker
def create_figure():
    p = figure(plot_height=600, plot_width=800, tools="pan,box_zoom,hover,reset", toolbar_location=None, sizing_mode="scale_width")
    if vsource.to_df().empty: return p
    p.quad(top='top', bottom='bottom', left='left', right='right', fill_color="navy", line_color="white", alpha=0.5, source = vsource)
    p.y_range.start = 0
    p.xaxis.axis_label = 'x'
    p.yaxis.axis_label = 'Vol(x)'
    p.grid.grid_line_color="white"
    return p

def update(industry, dtype, mdate):
    dtype_list = [dtype]
    if industry == '所有':
        df = stock_df
        print(mdate, dtype_list)
        return cvaluation.get_vertical_data(df, dtype_list, mdate)
    else:
        df = stock_df.loc[stock_df.industry == industry]
        df = df.reset_index(drop = True)
        print(mdate, dtype_list, industry)
        return cvaluation.get_vertical_data(df, dtype_list, mdate, industry)

@gen.coroutine
def locked_update(data, dtype):
    data = data.loc[data[dtype] > -5]
    values = data[dtype].tolist()
    hist, edges = np.histogram(values, density=False, bins=50)
    vsource.stream(dict(bottom = [0 for n in range(50)], top = hist, left = edges[0:len(edges)-1], right = edges[1:len(edges)]))
    layout.children[1] = create_figure()

@gen.coroutine
@without_document_lock
def unlocked_task():
    mdate = date_picker.value
    dtype = value_select.value
    industry = industry_select.value
    mdate = int(datetime_to_str(mdate))
    vdata = yield executor.submit(update, industry, dtype, mdate)
    cdoc.add_next_tick_callback(partial(locked_update, data=vdata, dtype = dtype))

cdoc = curdoc()
cvaluation = CValuation()
stock_info_client = CStockInfo()

vsource = ColumnDataSource(dict(bottom = list(), top = list(), left = list(), right = list()))

stock_df = stock_info_client.get()
industries = list(set(stock_df.industry.tolist()))
columns = ['roa', 'roe']
industries.append("所有")

date_picker = DatePicker(value = date.today() - timedelta(days = 200), min_date = date(2004,1,1), max_date = date.today())
industry_select = Select(value='所有', title='行业', options=sorted(industries))
value_select = Select(value='roe', title='类型', options=sorted(columns))

controls = row(industry_select, value_select)
layout = column(controls, create_figure())

executor = ThreadPoolExecutor(max_workers=2)

cdoc.add_root(layout)
cdoc.add_periodic_callback(unlocked_task, 40000)
cdoc.title = "指标分布"
