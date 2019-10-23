# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from futu import TrdEnv
from datetime import datetime
from algotrade.model.follow_trend import FollowTrendModel
from algotrade.broker.futu.fututrader import FutuTrader
from apscheduler.schedulers.gevent import GeventScheduler
def set_info(model_name = 'follow_trend'):
    mdate = datetime.now().strftime('%Y-%m-%d')
    model = FollowTrendModel(should_create_mysqldb = True)
    unlock_path_ = "/scode/configure/{}.json".format(model_name)
    futuTrader = FutuTrader(host = ct.FUTU_HOST, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, market = ct.CN_MARKET_SYMBOL, unlock_path = unlock_path_)
    if model.cal_client.is_trading_day(mdate):
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

#unlock_path_ = "/scode/configure/{}.json".format('follow_trend')
#futuTrader = FutuTrader(host = ct.FUTU_HOST, port = ct.FUTU_PORT, trd_env = TrdEnv.SIMULATE, market = ct.CN_MARKET_SYMBOL, unlock_path = unlock_path_)
#cash = futuTrader.get_cash()
#shares = futuTrader.get_shares()
#positons = futuTrader.get_postitions()
#total_asserts = futuTrader.get_total_assets()
