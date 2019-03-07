# coding=utf-8
import threading
class CThread(threading.Thread):
    def __init__(self, fun, param):
        threading.Thread.__init__(self)
        self.fun = fun
        self.param = param

    def run(self):
        self.fun(self.param)
