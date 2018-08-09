# coding=utf-8
class TickTradeDetail:
    def __init__(self, ctime, price, volume, ctype):
        self.dtime = ctime
        self.price = price
        self.volume = volume
        self.type = ctype

    def __repr__(self):
        return "time:%s, price:%s, volume:%s, type:%s" % (self.dtime, self.price, self.volume, self.type)

class TickDetailModel:
    def __init__(self, cdate, ctime, cprice, cvolume, ccount, ctype, cvol_offset, cvol_size):
        self.ddate = cdate
        self.dtime = ctime
        self.price = cprice
        self.volume = cvolume
        self.count = ccount
        self.type = ctype
        self.vol_offset = cvol_offset
        self.vol_size = cvol_size

    def __repr__(self):
        return "date:%s, time:%s, price:%s, volume:%s, count:%s, type:%s, vol_offset:%s, vol_size:%s" % (self.ddate, self.dtime, self.price, self.volume, self.count, self.type, self.vol_offset, self.vol_size)

class HashItem:
    def __init__(self, _index, _value):
        self.index = _index
        self.value= _value
