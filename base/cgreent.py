#coding=utf-8
from gevent import Greenlet
class CGreenlet(Greenlet):
    def __init__(self, name, *args, **kwargs):
        Greenlet.__init__(self, *args, **kwargs)
        self.name = name

    def __str__(self):
        return 'greenlet name:%s' % self.name
