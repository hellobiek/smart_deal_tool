# -*- coding: utf-8 -*-
from datetime import date
from random import randint
from ticker import MyTicker
from bokeh.core.properties import Int
from bokeh.plotting import figure, curdoc
from bokeh.layouts import widgetbox, row, column
from bokeh.models import ColumnDataSource, CustomJS
from bokeh.models.widgets import TextInput, DataTable, DateFormatter, TableColumn
fruits = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
p = figure(x_range=fruits, plot_height=250, title="Fruit Counts", tools="box_zoom")
p.vbar(x=fruits, top=[5, 3, 4, 2, 4, 6], width=0.9)
p.xgrid.grid_line_color = None
p.y_range.start = 0
p.xaxis.ticker = MyTicker(nth=1)
cb = CustomJS(args=dict(ticker=p.xaxis[0].ticker), code="""
    if (Math.abs(cb_obj.start-cb_obj.end) > 8) {
        ticker.nth = 2
    } else {
        ticker.nth = 1
    }
""")

p.x_range.js_on_change('start', cb)
p.x_range.js_on_change('end', cb)

cdoc = curdoc()
layout = row(p)
cdoc.add_root(layout)
