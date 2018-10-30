# -*- coding: utf-8 -*-
import pytz
import time
import threading
from datetime import datetime
from base.langconv import Converter
def traditional2simplified(sentence):
    return Converter('zh-hans').convert(sentence)

def get_today_time(stime):
    year, month,  day    = time.strptime(datetime.now().strftime('%Y-%m-%d'), "%Y-%m-%d")[0:3]
    hour, minute, second = time.strptime(stime, '%H:%M:%S')[3:6]
    return datetime(year, month, day, hour, minute, second) 

def localnow(timezone):
    return datetime.strptime(datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

class PollingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stopped  = True

    def start(self):
        self.stopped = False
        threading.Thread.start(self)

    def stop(self):
        self.stopped = True

    def wait(self):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()

    def getNextCallDateTime(self):
        raise NotImplementedError()

    def doCall(self):
        raise NotImplementedError()
