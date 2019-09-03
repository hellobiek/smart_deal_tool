# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import dash
import dash_table
import const as ct
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime
from base.cobj import CMysqlObj
from flask_caching import Cache
from dash.dependencies import Input, Output
from algotrade.model.qmodel import QModel
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
    dcc.DatePickerSingle(
        id = 'single-date-picker',
        min_date_allowed = datetime(2000, 1, 1),
        max_date_allowed = datetime.now(),
        initial_visible_month = datetime.now(),
        date = str(datetime.now())
    ),
    html.Div(id='output-container-single-date-picker', style={'display': 'none'}),
    dcc.Tabs(id="tabs", value='tabs', children=[
        dcc.Tab(label='中位控盘', value='follow_trend'),
        dcc.Tab(label='低位吸筹', value='low-control'),
        dcc.Tab(label='否极泰来', value='depth-will'),
    ]),
    html.Div(id='tabs-content')
])

@cache.memoize()
def get_data(model, mdate):
    model = QModel(code = model, dbinfo = ct.OUT_DB_INFO, redis_host = "127.0.0.1")
    df = model.get_data(mdate)
    return df

@app.callback(
    Output('output-container-single-date-picker', 'children'),
    [Input('single-date-picker', 'date')])
def update_output(date):
    if date is not None:
        return date.split(' ')[0]

@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value'), Input('output-container-single-date-picker', 'children')])
def render_content(model, mdate):
    if model == 'follow_trend':
        df = get_data(model, mdate)
        return html.Div([
            html.H3('股票池'),
            dash_table.DataTable(
                id = 'follow_trend-stock-pool',
                columns = [{"name": i, "id": i} for i in df.columns],
                data = df.to_dict('records'),
            )
        ])
    elif model == 'low-control':
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
    elif model == 'depth-will':
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
