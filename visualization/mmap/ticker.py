# -*- coding: utf-8 -*-
from bokeh.core.properties import Int
from bokeh.models import CategoricalTicker
class MyTicker(CategoricalTicker):
    __implementation__ = """
    import {CategoricalTicker} from "models/tickers/categorical_ticker"
    import * as p from "core/properties"
    export class MyTicker extends CategoricalTicker
        type: "MyTicker"
        @define {
            nth: [ p.Int, 1 ]
        }
        get_ticks: (start, end, range, cross_loc) ->
            ticks = super(start, end, range, cross_loc)
            ticks.major = ticks.major.filter((element, index) => index % this.nth == 0)
            return ticks
    """
    nth = Int(default=1)
