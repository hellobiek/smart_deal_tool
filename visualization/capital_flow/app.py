# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import dash
import dash_table
import const as ct
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from flask_caching import Cache
from common import str_of_num 
from rstock import RIndexStock
from cstock_info import CStockInfo
from visualization.dash.hgt import HGT
from visualization.dash.rzrq import RZRQ
from datetime import datetime, timedelta
from dash.dependencies import Input, Output
top100 = None
add_data = None
del_data = None
redis_host = "127.0.0.1"
dbinfo = ct.OUT_DB_INFO
mstart = None
mend = None
model_dir = "/Volumes/data/quant/stock/data/models"
report_dir = "/Volumes/data/quant/stock/data/tdx/report"
cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
tushare_file_path = "/Users/hellobiek/Documents/workspace/python/quant/smart_deal_tool/configure/tushare.json"

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets, suppress_callback_exceptions = True)
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_URL': '127.0.0.1:6579'
}
cache = Cache()
cache.init_app(app.server, config=CACHE_CONFIG)
app.layout = html.Div([
    html.H1('资金流情况'),
    dcc.DatePickerRange(
        id = 'date-picker-range',
        min_date_allowed = datetime(2017, 1, 1),
        max_date_allowed = datetime.now(),
        initial_visible_month = datetime.now(),
        start_date = datetime.now() - timedelta(7),
        end_date = datetime.now()
    ),
    html.Div(id='output-start-date', style={'display': 'none'}),
    html.Div(id='output-end-date', style={'display': 'none'}),
    dcc.Tabs(id="tabs", value='tabs', children=[
        dcc.Tab(label='港股通', value='hk-flow'),
        dcc.Tab(label='融资融券', value='leveraged-funds'),
    ]),
    html.Div(id='hold-situation', children='hgt-hold-situation-table'),
])

@cache.memoize()
def get_money_flow_data_from_rzrq(code, start, end):
    rzrq_client = RZRQ(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host, fpath = tushare_file_path)
    data = rzrq_client.get_data(code, start, end)
    return data

@cache.memoize()
def get_top20_stock_info_from_hgt(cdate):
    hgt_client = HGT(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host)
    info = hgt_client.get_top10_info(cdate)
    info['net_turnover'] = info['buy_turnover'] - info['sell_turnover']
    info = info.sort_values(by = 'net_turnover', ascending= False)
    info = info.drop('rank', axis = 1)
    info = info.reset_index(drop = True)
    info['total_turnover'] = info['total_turnover'].apply(lambda x:str_of_num(x))
    info['net_turnover'] = info['net_turnover'].apply(lambda x:str_of_num(x))
    info['buy_turnover'] = info['buy_turnover'].apply(lambda x:str_of_num(x))
    info['sell_turnover'] = info['sell_turnover'].apply(lambda x:str_of_num(x))
    return info

@cache.memoize()
def get_money_flow_data_from_hgt(start, end):
    hgt_client = HGT(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host)
    sh_data = hgt_client.get_data("ALL_SH", start, end)
    sz_data = hgt_client.get_data("ALL_SZ", start, end)
    if start not in sh_data.date.tolist():
        return None, None, "{} 没有数据".format(start)
    if end not in sh_data.date.tolist():
        return None, None, "{} 没有数据".format(end)
    sh_data = sh_data.loc[(sh_data.date == start) | (sh_data.date == end)]
    sz_data = sz_data.loc[(sz_data.date == start) | (sz_data.date == end)]
    sh_data = sh_data.append(sz_data)
    sh_data = sh_data.reset_index(drop = True)
    rstock = RIndexStock(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host)
    rstock_info = rstock.get_data(end)
    rstock_info = rstock_info[['code', 'totals']]
    stock_info_client = CStockInfo(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host, stocks_dir = stocks_dir, base_stock_path = base_stock_path)
    base_df = stock_info_client.get()
    base_df = base_df[['code', 'timeToMarket', 'industry', 'sw_industry']]
    rstock_info = pd.merge(rstock_info, base_df, how='inner', on=['code'])
    df = pd.merge(sh_data, rstock_info, how='left', on=['code'])
    df = df.dropna(axis=0, how='any')
    df = df.reset_index(drop = True)
    df['percent'] = 100 * df['volume'] / df['totals']
    df = df[['date', 'code', 'name', 'timeToMarket', 'industry', 'sw_industry', 'percent', 'volume', 'totals']]
    start_data = df.loc[df.date == start]
    start_data = start_data.sort_values(by = 'percent', ascending= False)
    start_data = start_data.reset_index(drop = True)
    end_data = df.loc[df.date == end]
    end_data = end_data.sort_values(by = 'percent', ascending= False)
    end_data = end_data.reset_index(drop = True)
    top100 = end_data.loc[end_data.percent > 5]
    top100 = top100.reset_index(drop = True)
    top100['percent'] = round(top100['percent'], 2)
    start_data = start_data[['code', 'percent']]
    start_data = start_data.rename(columns = {"percent": "spercent"})
    cdata = pd.merge(end_data, start_data, how='left', on=['code'])
    cdata = cdata.dropna(axis=0, how='any')
    cdata['delta_percent'] = cdata['percent'] - cdata['spercent']
    cdata = cdata[['date', 'code', 'name', 'timeToMarket', 'industry', 'sw_industry', 'delta_percent', 'volume', 'totals']]
    cdata['delta_percent'] = round(cdata['delta_percent'], 2)
    cdata = cdata.sort_values(by = 'delta_percent', ascending= False)
    cdata = cdata.reset_index(drop = True)
    add_data = cdata.loc[cdata.delta_percent > 0]
    add_data = add_data.sort_values(by = 'delta_percent', ascending= False)
    add_data = add_data.head(30)
    add_data = add_data.reset_index(drop = True)
    del_data = cdata.loc[cdata.delta_percent < 0]
    del_data = del_data.sort_values(by = 'delta_percent', ascending= True)
    del_data = del_data.head(30)
    del_data = del_data.reset_index(drop = True)
    return top100, add_data, del_data

@app.callback(
    [Output('output-start-date', 'children'), Output('output-end-date', 'children')],
    [Input('date-picker-range', 'start_date'), Input('date-picker-range', 'end_date')])
def update_date(start_date, end_date):
    global mstart, mend
    if start_date is not None and end_date is not None:
        mstart = start_date.split(' ')[0]
        mend = end_date.split(' ')[0]
        return mstart, mend
    return None, None

@app.callback(Output('hold-situation', 'children'),
              [Input('tabs', 'value'), Input('output-start-date', 'children'), Input('output-end-date', 'children')])
def render_content(model_name, start_date, end_date):
    global top100, add_data, del_data
    if model_name == 'hk-flow':
        top100, add_data, del_data = get_money_flow_data_from_hgt(start_date, end_date)
        top20_info = get_top20_stock_info_from_hgt(end_date)
        if top20_info is None or top20_info.empty:
            return html.Div([html.H3('{} : 二十大热门股没有数据'.format(end_date))])
        else:
            if top100 is None:
                return html.Div([
                    html.H3('{}日的20大成交额股票（按照净买入额排序）'.format(end_date)),
                    dash_table.DataTable(
                        id = 'hgt-top20-data',
                        columns = [{"name": i, "id": i} for i in top20_info.columns],
                        data = top20_info.to_dict('records'),
                        style_cell={'textAlign': 'center'},
                        sort_action = "native",
                    ),
                    html.H3('{}: 港股通数据有错误'.format(end_date))])
            else:
                return html.Div([
                    html.H3('{}日的20大成交额股票（按照净买入额排序）'.format(end_date)),
                    dash_table.DataTable(
                        id = 'hgt-top20-data',
                        columns = [{"name": i, "id": i} for i in top20_info.columns],
                        data = top20_info.to_dict('records'),
                        style_cell={'textAlign': 'center'},
                        sort_action = "native",
                    ),
                    html.H3('{}日持股比例最多的100只股票(持有股本/总股本)'.format(end_date)),
                    dash_table.DataTable(
                        id = 'hgt-data',
                        columns = [{"name": i, "id": i} for i in top100.columns],
                        data = top100.to_dict('records'),
                        style_cell={'textAlign': 'center'},
                        sort_action = "native",
                    ),
                    html.H3('持股比例增加最多的30只股票(持有股本/总股本)'),
                    dash_table.DataTable(
                        id = 'hgt-add-data',
                        columns = [{"name": i, "id": i} for i in add_data.columns],
                        data = add_data.to_dict('records'),
                        style_cell={'textAlign': 'center'},
                        sort_action = "native",
                    ),
                    html.H3('持股比例减少最多的30只股票(持有股本/总股本)'),
                    dash_table.DataTable(
                        id = 'hgt-del-data',
                        columns = [{"name": i, "id": i} for i in del_data.columns],
                        data = del_data.to_dict('records'),
                        style_cell={'textAlign': 'center'},
                        sort_action = "native",
                    ),
                ])
    elif model_name == 'leveraged-funds':
        return html.Div([
            html.H3('Tab content 2'),
            dcc.Graph(
                id = 'graph-2-tabs',
                figure = {
                    'data': [{
                        'x': [1, 2, 3],
                        'y': [5, 10, 6],
                        'type': 'bar'
                    }]
                }
            )
        ])

if __name__ == '__main__':
    app.run_server(debug = True, port = 9998)
