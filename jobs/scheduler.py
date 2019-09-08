# -*- coding: utf-8 -*-
from datetime import datetime
from apscheduler.schedulers.blocking import GeventScheduler
scheduler = GeventScheduler()

@scheduler.scheduled_job('cron', day_of_week = 'mon-fri', hour = '15', minute = '01')
def record():
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

for job in jobs:
    scheduler.add_job(func = job['func'], args = job['args'], trigger = 'cron', day_of_week = 'mon-fri', hour = 5, minute = 30)
scheduler.start()
