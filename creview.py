#coding=utf-8
import time
import datetime
from datetime import datetime
import const as ct
import pandas as pd
import tushare as ts
import cstock_info as cs_info
from cmysql import CMySQL
from log import getLogger
from common import trace_func

logger = getLogger(__name__)

class CWatchDog():
    @trace_func(log = logger)
    def __init__(self):
        pass

    @trace_func(log = logger)
    def create(self):
        pass

    @trace_func(log = logger)
    def prepare(self, sleep_time):
        pass

    @trace_func(log = logger)
    def init(self):
        pass

    @trace_func(log = logger)
    def run(self, dtype, sleep_time):
        pass

    # collect index info
    # collect concept info
    # collect average price
    # collect daily statics
    # see daily happens
