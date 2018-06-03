import sys
import time
import ccalendar
import cstock
import cstock_info
import combination
import combination_info
import const as ct
import pandas as pd
from log import getLogger
from common import trace_func

pd.options.mode.chained_assignment = None #default='warn'
pd.set_option('max_rows', 200)
logger = getLogger(__name__)

class CBase:
    @trace_func(log = logger)
    def __init__(self, dbinfo, stock_info_table, combination_info_table, calendar_table):
        self.cal_client = ccalendar.CCalendar(dbinfo, calendar_table)
        self.comb_info_client = combination_info.CombinationInfo(dbinfo, combination_info_table)
        self.stock_info_client = cstock_info.CStockInfo(dbinfo, stock_info_table) 
   
