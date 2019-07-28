# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from datetime import datetime
from bokeh.models import Select
from bokeh.layouts import row, column
from bokeh.plotting import curdoc, figure
def get_all_codes(spath, tpath):
    spath_obj = Path(spath)
    tpath_obj = Path(tpath)
    source_csvs = spath_obj.glob('*.csv')
    source_files = [xfile.name.split('_')[0] for xfile in source_csvs]
    source_files.sort()
    source_codes = [xx[1:] for xx in source_files]
    source_codes_set = set(source_codes)
    
    target_csvs = tpath_obj.glob('*.csv')
    target_files = [xfile.name.split('_')[0] for xfile in target_csvs]
    target_files.sort()
    target_codes = [xx[1:] for xx in target_files]
    target_codes_set = set(target_codes)
    return list(source_codes_set.intersection(target_codes_set))

def str_to_datetime(mdate:str, dformat = "%Y%m%d"):
    return datetime.strptime(mdate, dformat)

def int_to_datetime(mdate:int, dformat = "%Y%m%d"):
    return str_to_datetime(str(mdate), dformat)

def get_val_filename(code):
    return "s%s_val.csv" % code

def read_data(filepath, filename):
    path_obj = Path(filepath)
    fpath = path_obj / filename
    if path_obj.name == 'stocks':
        DATA_COLUMS = ['date', 'pe', 'ttm', 'pb', 'dr']
        DTYPE_DICT = {'date': int, 'pe': float, 'ttm': float, 'pb': float, 'dr': float}
        df = pd.read_csv(fpath, header = 0, encoding = "utf8", usecols = DATA_COLUMS, dtype = DTYPE_DICT)
        df['date'] = df['date'].apply(lambda x:int_to_datetime(x))
        df['dr'] = df['dr'] * 100
    else:
        DATA_COLUMS = ['date', 'pe', 'ttm', 'pb', 'dividend']
        DTYPE_DICT = {'date': str, 'pe': float, 'ttm': float, 'pb': float, 'dividend': float}
        df = pd.read_csv(fpath, header = 0, encoding = "utf8", usecols = DATA_COLUMS, dtype = DTYPE_DICT)
        df['date'] = df['date'].apply(lambda x:str_to_datetime(x, "%Y-%m-%d"))
        df = df.rename(columns={'dividend': 'dr'})
    return df
    
def create_figure():
    code = code_select.value
    colname = value_select.value
    filename = get_val_filename(code)
    sdf = read_data(spath, filename)
    tdf = read_data(tpath, filename)
    mdf = pd.merge(sdf, tdf, on = 'date', how = 'inner', suffixes=["_s", "_t"])
    p = figure(plot_height=600, plot_width=800, tools="pan,box_zoom,hover,reset", toolbar_location=None, sizing_mode="scale_width")
    p.xaxis.major_label_overrides = {i: mdate.strftime('%Y-%m-%d') for i, mdate in enumerate(mdf["date"])}

    p.xaxis.axis_label = "时间"
    p.yaxis.axis_label = colname

    p.line(mdf.index, mdf["%s_s" % colname], line_width = 3, line_color="blue", alpha=0.6, hover_color='black', hover_alpha=0.5)
    p.line(mdf.index, mdf["%s_t" % colname], line_width = 3, line_color="green", alpha=0.6, hover_color='black', hover_alpha=0.5)
    return p

def update(attr, old, new):
    layout.children[1] = create_figure()

columns = ['pe','ttm','pb','dr']
spath = '/Volumes/data/quant/stock/data/valuation/stocks'
tpath = '/Volumes/data/quant/stock/data/valuation/cstocks'
codes = get_all_codes(spath, tpath)

code_select = Select(value='000001', title='代码', options=sorted(codes))
value_select = Select(value='pe', title='价值类型', options=sorted(columns))

code_select.on_change('value', update)
value_select.on_change('value', update)

controls = row(code_select, value_select)
layout = column(controls, create_figure())

curdoc().add_root(layout)
curdoc().title = "价值对比"
