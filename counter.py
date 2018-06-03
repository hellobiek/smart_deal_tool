#coding=utf-8
import time
import datetime
from datetime import datetime
import const as ct
import numpy as np
import pandas as pd
import tushare as ts
from log import getLogger
from cmysql import CMySQL
from common import trace_func, _fprint

logger = getLogger(__name__)

class CCounter:
    @trace_func(log = logger)
    def __init__(self, dbinfo, table_name):
        pass
