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
from futu import TrdEnv
from flask_caching import Cache
from visualization.dash.hgt import HGT
from visualization.dash.rzrq import RZRQ
from datetime import datetime, timedelta
from dash.dependencies import Input, Output
from algotrade.broker.futu.fututrader import FutuTrader
from algotrade.model.follow_trend import FollowTrendModel
data_source = None
redis_host = "127.0.0.1"
dbinfo = ct.OUT_DB_INFO
mstart = None
mend = None
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
    html.H1('模型'),
    dcc.DatePickerRange(
        id = 'date-picker-range',
        min_date_allowed = datetime(2000, 1, 1),
        max_date_allowed = datetime.now(),
        initial_visible_month = datetime.now(),
        start_date = datetime.now() - timedelta(190),
        end_date = datetime.now()
    ),
    html.Div(id='output-start-date', style={'display': 'none'}),
    html.Div(id='output-end-date', style={'display': 'none'}),
    dcc.Tabs(id="tabs", value='tabs', children=[
        dcc.Tab(label='中位控盘', value='follow_trend'),
        dcc.Tab(label='低位吸筹', value='low-control'),
        dcc.Tab(label='否极泰来', value='depth-will'),
    ]),
    html.Div(id='model-situation', children='model-situation-table'),
    html.Div([
            html.H3('资金情况'),
            html.Div(id='model-situation-hgt-container', children='model-situation-hgt-graph'),
            html.Div(id='model-situation-rzrq-container', children='model-situation-rzrq-graph')
    ])
])

@cache.memoize()
def get_money_flow_data_from_rzrq(code, start, end):
    rzrq_client = RZRQ(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host, fpath = tushare_file_path)
    data = rzrq_client.get_data(code, start, end)
    return data

@cache.memoize()
def get_money_flow_data_from_hgt(code, start, end):
    hgt_client = HGT(dbinfo = ct.OUT_DB_INFO, redis_host = redis_host)
    data = hgt_client.get_data(code, start, end)
    return data

@cache.memoize()
def get_profit_data(model, start, end):
    accinfos = model.get_account_info(end, end)
    positions = model.get_position_info(end, end)
    orders = model.get_history_order_info(end, end)
    profits = model.get_account_info(start, end)
    return accinfos, positions, orders, profits

@cache.memoize()
def get_data(model, mdate):
    ndf = model.get_stock_pool(mdate)
    pdate = model.cal_client.pre_trading_day(mdate)
    pdf = model.get_stock_pool(pdate)
    if not ndf.empty and not pdf.empty:
        adf = ndf[~ndf.code.apply(tuple,1).isin(pdf.code.apply(tuple,1))]
        adf['status'] = '增加'
        ddf = pdf[~pdf.code.apply(tuple,1).isin(ndf.code.apply(tuple,1))]
        reason_list = list()
        for _, code in ddf.code.iteritems():
            reason_list.append('减少：{}'.format(model.get_deleted_reason(code, mdate)))
        ddf['status'] = reason_list
        ndf = ndf[~ndf.code.apply(tuple,1).isin(adf.code.apply(tuple,1))]
        ndf['status'] = '维持'
        ndf = ndf.append(adf)
        ndf = ndf.append(ddf)
        ndf = ndf.sort_values(['industry'], ascending = 1)
        ndf = ndf.reset_index(drop = True)
        return ndf
    elif not ndf.empty and pdf.empty:
        ndf['status'] = '增加'
        ndf = ndf.sort_values(['industry'], ascending = 1)
        ndf = ndf.reset_index(drop = True)
        return ndf
    elif ndf.empty and not pdf.empty:
        pdf['date'] = mdate
        reason_list = list()
        for _, code in pdf.code.iteritems():
            reason_list.append('减少：{}'.format(model.get_deleted_reason(code, mdate)))
        pdf['status'] = reason_list
        pdf = pdf.sort_values(['industry'], ascending = 1)
        pdf = pdf.reset_index(drop = True)
        return pdf
    else:
        return pd.DataFrame()

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

@app.callback([Output('model-situation-hgt-container', 'children'), Output('model-situation-rzrq-container', 'children')],
              [Input('follow_trend-stock-pool', 'active_cell')])
def update_graphs(active_cell):
    if active_cell is None: return None, None
    if data_source is None: return None, None
    global mstart, mend
    row_id = active_cell['row']
    row = data_source.loc[row_id].to_dict()
    code = row['code']
    name = row['name']
    money_hgt = get_money_flow_data_from_hgt(code, mstart, mend)
    if money_hgt.empty:
        hgt_figure = "{} 不是沪港通标的".format(code)
    else:
        hgt_figure = dcc.Graph(
            id = 'hgt-money-flow',
            figure = {
                'data': [{
                    'x': money_hgt.date.tolist(),
                    'y': money_hgt.percent.tolist(),
                    'mode': "lines+markers",
                    'line': {"color": "#f4d44d"}
                }],
                'layout': {
                    'title': '{} 港股通资金流向'.format(name)
                }
            }
        )
    money_rzrq = get_money_flow_data_from_rzrq(code, mstart, mend)
    if money_rzrq.empty:
        rzrq_figure = "{} 不是融资融券标的".format(code)
    else:
        rzrq_figure = dcc.Graph(
            id = 'rzrq-money-flow',
            figure = {
                'data': [{
                    'x': money_rzrq.date.tolist(),
                    'y': money_rzrq.rzrqye.tolist(),
                    'mode': "lines+markers",
                    'line': {"color": "#f4d44d"}
                }],
                'layout': {
                    'title': '{} 融资融券资金流向'.format(name)
                }
            },
        )
    return hgt_figure, rzrq_figure

@app.callback(Output('model-situation', 'children'),
              [Input('tabs', 'value'), Input('output-start-date', 'children'), Input('output-end-date', 'children')])
def render_content(model_name, start_date, end_date):
    global data_source
    if model_name == 'follow_trend':
        model = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo, redis_host = redis_host)
        acc_df, pos_df, order_df, profit_df = get_profit_data(model, start_date, end_date)
        data_source = get_data(model, end_date)
        return html.Div([
            html.H3('股票池'),
            dash_table.DataTable(
                id = 'follow_trend-stock-pool',
                columns = [{"name": i, "id": i} for i in data_source.columns],
                data = data_source.to_dict('records'),
                style_cell = {'textAlign': 'left'},
                sort_action = "native",
                selected_rows = [],
                style_cell_conditional = [
                    {'if': {'column_id': 'date'}, 'width': '10%',},
                    {'if': {'column_id': 'code'}, 'width': '10%',},
                    {'if': {'column_id': 'name'}, 'width': '15%',},
                    {'if': {'column_id': 'industry'}, 'width': '10%',},
                    {'if': {'column_id': 'status'}, 'width': '20%',}
                ],
                style_data_conditional=[{
                    'if': {'filter_query': '{status} eq "增加"',},
                    'backgroundColor': 'rgb(0, 0, 50)',
                    'color': 'white'
                },{
                    'if': {'filter_query': '{status} contains "减少"',},
                    'backgroundColor': 'rgb(0, 50, 0)',
                    'color': 'white'
                }]
            ),
            html.H3('账户信息'),
            dash_table.DataTable(
                id = 'follow_trend-account',
                columns = [{"name": i, "id": i} for i in acc_df.columns],
                data = acc_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('仓位信息'),
            dash_table.DataTable(
                id = 'follow_trend-position',
                columns = [{"name": i, "id": i} for i in pos_df.columns],
                data = pos_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('今日订单'),
            dash_table.DataTable(
                id = 'follow_trend-order',
                columns = [{"name": i, "id": i} for i in order_df.columns],
                data = order_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('历史收益率'),
            dcc.Graph(
                id = 'follow_trend-profit',
                figure = {
                    'data': [{
                        'x': profit_df.date.tolist(),
                        'y': profit_df.total_assets.tolist(),
                        'mode': "lines+markers",
                        'line': {"color": "#f4d44d"}
                    }]
                }
            )
        ])
    elif model_name == 'low-control':
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
    elif model_name == 'depth-will':
        return html.Div([
            html.H3('Tab content 3'),
            dcc.Graph(
                id = 'graph-3-tabs',
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
    app.run_server(debug = True, port = 9999)
