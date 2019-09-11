# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from futu import TrdEnv
from datetime import datetime
from algotrade.model.qmodel import QModel
from algotrade.broker.futu.fututrader import FutuTrader
from apscheduler.schedulers.gevent import GeventScheduler
def set_info(model = 'follow_trend'):
    mdate = datetime.now().strftime('%Y-%m-%d')
    unlock_path_ = "/scode/configure/{}.json".format(model)
    model = QModel(code = model, should_create_mysqldb = True)
    futuTrader = FutuTrader(host = ct.FUTU_HOST, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, market = ct.CN_MARKET_SYMBOL, unlock_path = unlock_path_)
    model.set_account_info(mdate, futuTrader)
    model.set_position_info(mdate, futuTrader)
    model.set_history_order_info(mdate, futuTrader)

class Scheduler(object):
    def __init__(self):
        self.scheduler = GeventScheduler()
        self.add_jobs()

    def start(self):
        return self.scheduler.start()

    def add_jobs(self):
        self.scheduler.add_job(set_info, 'cron', day_of_week='mon-fri', hour=15, minute=5)
