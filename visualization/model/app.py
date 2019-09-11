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
from datetime import datetime, timedelta
from algotrade.model.qmodel import QModel
from dash.dependencies import Input, Output
from algotrade.broker.futu.fututrader import FutuTrader
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
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
        start_date = datetime.now() - timedelta(10),
        end_date = datetime.now()
    ),
    html.Div(id='output-start-date', style={'display': 'none'}),
    html.Div(id='output-end-date', style={'display': 'none'}),
    dcc.Tabs(id="tabs", value='tabs', children=[
        dcc.Tab(label='中位控盘', value='follow_trend'),
        dcc.Tab(label='低位吸筹', value='low-control'),
        dcc.Tab(label='否极泰来', value='depth-will'),
    ]),
    html.Div(id='model-situation'),
])

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
        ddf['status'] = '减少'
        ndf = ndf[~ndf.code.apply(tuple,1).isin(adf.code.apply(tuple,1))]
        ndf['status'] = '维持'
        ndf = ndf.append(adf)
        ndf = ndf.append(ddf)
        return ndf
    elif not ndf.empty and pdf.empty:
        ndf['status'] = '增加'
        return ndf
    elif ndf.empty and not pdf.empty:
        pdf['date'] = mdate
        pdf['status'] = '减少'
        return pdf
    else:
        return pd.DataFrame()

@app.callback(
    [Output('output-start-date', 'children'), Output('output-end-date', 'children')],
    [Input('date-picker-range', 'start_date'), Input('date-picker-range', 'end_date')])
def update_date(start_date, end_date):
    if start_date is not None and end_date is not None:
        return start_date.split(' ')[0], end_date.split(' ')[0]
    return None, None

@app.callback(Output('model-situation', 'children'),
              [Input('tabs', 'value'), Input('output-start-date', 'children'), Input('output-end-date', 'children')])
def render_content(model_name, start_date, end_date):
    if model_name == 'follow_trend':
        model = QModel(code = model_name, dbinfo = ct.OUT_DB_INFO, redis_host = "127.0.0.1", cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv")
        df = get_data(model, end_date)
        acc_df, pos_df, order_df, profit_df = get_profit_data(model, start_date, end_date)
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA01")
        print(start_date, end_date)
        print(acc_df, pos_df, order_df, profit_df)
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA02")
        return html.Div([
            html.H3('股票池'),
            dash_table.DataTable(
                id = 'follow_trend-stock-pool',
                columns = [{"name": i, "id": i} for i in df.columns],
                data = df.to_dict('records'),
                style_cell = {'textAlign': 'left'},
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
                    'if': {'filter_query': '{status} eq "减少"',},
                    'backgroundColor': 'rgb(0, 50, 0)',
                    'color': 'white'
                }]
            ),
            html.H3('账户信息'),
            dash_table.DataTable(
                id='follow_trend-account',
                columns = [{"name": i, "id": i} for i in acc_df.columns],
                data = acc_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('仓位信息'),
            dash_table.DataTable(
                id='follow_trend-position',
                columns = [{"name": i, "id": i} for i in pos_df.columns],
                data = pos_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('今日订单'),
            dash_table.DataTable(
                id='follow_trend-order',
                columns = [{"name": i, "id": i} for i in order_df.columns],
                data = order_df.to_dict('records'),
                style_cell = {'textAlign': 'left'}
            ),
            html.H3('历史收益率'),
            dcc.Graph(
                id='follow_trend-profit',
                figure={
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
                id='graph-2-tabs',
                figure={
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
                id='graph-3-tabs',
                figure={
                    'data': [{
                        'x': [1, 2, 3],
                        'y': [5, 10, 6],
                        'type': 'bar'
                    }]
                }
            )
        ])

if __name__ == '__main__':
    app.run_server(debug=True, port=9999)
