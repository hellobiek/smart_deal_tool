# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import colorlover as cl
import dash_core_components as dcc
from cstock import CStock
from base.cdate import str_to_datetime 
colorscale = cl.scales['3']['qual']['Set1']
def get_graph(code = '601318', stock_name = '中国平安'):
    graphs = []
    obj = CStock(code)
    df = obj.get_k_data()
    df['date'] = df['date'].apply(lambda x:str_to_datetime(x, "%Y-%m-%d"))
    candlestick = {
        'x': df['date'],
        'open': df['open'],
        'high': df['high'],
        'low': df['low'],
        'close': df['close'],
        'type': 'candlestick',
        'increasing': {'line': {'color': colorscale[0]}},
        'decreasing': {'line': {'color': colorscale[2]}}
    }
    graphs.append(dcc.Graph(
        id = code,
        figure = {
            'data': [candlestick],
            'layout': {
                'title': stock_name,
                'yaxis': {'title': 'Stock Price (USD)'},
                'xaxis': {'type': 'category', 'categoryorder': 'category ascending'},
            }
        }
    ))
    return graphs
