# -*- coding: utf-8 -*-
import pytz
import threading
from datetime import datetime
from base.langconv import Converter
def traditional2simplified(sentence):
    return Converter('zh-hans').convert(sentence)

def localnow(timezone):
    return datetime.strptime(datetime.now(pytz.timezone('EST')).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

class PollingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stopped  = True

    def is_stopped(self):
        return self.stopped

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
