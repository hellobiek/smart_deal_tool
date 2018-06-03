# -*- coding: utf-8 -*-
# 2015.11.21 FUTU API 第一个版本
# 2015.12.17 增加buy,sell,get_depth,cancel基本函数
# 2015.12.18 验证buy,sell函数模拟单，不过改单和修改单还没验证通过
# 2015.12.22 验证modify,cancel函数模拟单
# 2016.02.27 FUTU API 第二版本 支持美股,增加资产和订单调用
# ver: 1.1
# developer：zmworm
# mail : zmworm<A>gmail.com
# public wechat id(微信公众号)：zhaonote
import json
import socket
import thread
import math
import traceback
from const import FUTU_HOST, FUTU_PORT 

# futnn plubin会开启本地监听服务端
# 请求及发送数据都是json格式, 具体详见插件的协议文档

def cmp_stockcode(s1, s2):
    return str(s1).lower() == str(s2).lower()

class Futu():
    def __init__(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((FUTU_HOST, FUTU_PORT))
        self.lock = thread.allocate_lock()

    def __del__(self):
        self.s.close()

    def __get_market_code(self, market_name):
        if market_name == 'hk':
            return 1
        if market_name == 'us':
            return 2
        if market_name == 'sh':
            return 3
        if market_name == 'sz':
            return 4
        return market_name

    def __socket_call(self, command, param):
        self.lock.acquire()
        try:
            req = {'Protocol': str(command),
                   'ReqParam': param,
                   'Version': '1'}
            mystr = json.dumps(req) + '\n'
            self.s.send(mystr)
            rsp = ""
            buf = self.s.recv(40960)
            # print buf
            mybuf = buf.split("\r\n")
            for rsp in mybuf:
                if len(rsp) > 2:
                    try:
                        rsp = rsp.decode('utf-8')
                    except Exception, e:
                        rsp = rsp.decode('gbk')
                    # print rsp
                    r = json.loads(rsp)
                    if r["Protocol"] == '6003' or r["Protocol"] == '6004'\
                            or r["Protocol"] == '6005':
                        if r['ErrCode'] <> '0':
                            print r['ErrCode'], r['ErrDesc']
                    elif r['ErrCode'] == '0':
                        self.lock.release()
                        return r["RetData"]
                    else:
                        print r['ErrCode'], r['ErrDesc']
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
        self.lock.release()
        return None

    def buy(self, market, stockcode, price, amount, order_type=-1, envtype=0):
        '''
        order_type: 0 增强限价(港) 1 市价,2 限价,51 盘前,52 盘后
        '''
        print "buy"
        if market == "hk":
            cmd = 6003
            if order_type == -1:
                order_type = 0
        elif market == "us":
            cmd = 7003
            if order_type == -1:
                order_type = 2
        req = {
            'Cookie': '123456',
            'OrderSide': '0',
            'OrderType': str(order_type),
            'Price': str(int(math.floor(price * 1000))),
            'Qty': str(amount),
            'StockCode': str(stockcode),
            'EnvType': str(envtype)
        }
        print req
        result = self.__socket_call(cmd, req)
        if result <> None and market == "us":
            result['OrderID'] = result['LocalID']
        return result

    def sell(self, market, stockcode, price, amount, order_type=2, envtype=0):
        print "sell"
        if market == "hk":
            cmd = 6003
            if order_type == -1:
                order_type = 0
        elif market == "us":
            cmd = 7003
            if order_type == -1:
                order_type = 2
        req = {
            'Cookie': '123456',
            'OrderSide': '1',
            'OrderType': str(order_type),
            'Price': str(int(math.floor(price * 1000))),
            'Qty': str(amount),
            'StockCode': str(stockcode),
            'EnvType': str(envtype)
        }
        result = self.__socket_call(cmd, req)
        if result <> None and market == "us":
            result['OrderID'] = result['LocalID']
        return result

    def get_depth(self, market, stockcode, depth=5):
        req = {
            'Market': str(self.__get_market_code(market)),
            'StockCode': str(stockcode),
            'GetGearNum': str(depth)
        }
        data = self.__socket_call(1002, req)
        if(data is None):
            return None
        for i in data['GearArr']:
            i['BuyPrice'] = round(float(i['BuyPrice']) / 1000, 3)
            i['BuyVol'] = int(i['BuyVol'])
            i['SellPrice'] = round(float(i['SellPrice']) / 1000, 3)
            i['SellVol'] = int(i['SellVol'])
        return data['GearArr']

    def get_ticker(self, market, stockcode):
        req = {
            'Market': str(self.__get_market_code(market)),
            'StockCode': str(stockcode),
        }
        data = self.__socket_call(1001, req)
        if(data is None):
            return None
        for i in ('Cur', 'High', 'Low', 'Close', 'Open', 'LastClose', 'Turnover'):
            data[i] = round(float(data[i]) / 1000, 3)
        data['Vol'] = int(data['Vol'])
        return data

    def cancel(self, market, order_id, envtype=0):
        if market == "hk":
            req = {
                'Cookie': '123456',
                'OrderID': str(order_id),
                'SetOrderStatusHK': '0',
                'EnvType': str(envtype),
            }
            cmd = 6004
        elif market == "us":
            req = {
                'Cookie': '123456',
                "LocalID":str(order_id),
                "OrderID": "0",
                "SetOrderStatus":"0",
                "EnvType":str(envtype)
            }
            cmd = 7004

        data = self.__socket_call(cmd, req)
        return data

    def modify(self, market, order_id, price, amount, envtype=0):
        if market == "hk":
            cmd = 6005
            req = {
                'Cookie': '12345',
                'OrderID': str(order_id),
                'Price': str(int(math.floor(price * 1000))),
                'Qty': str(amount),
                'EnvType': str(envtype),
            }
        elif market == "us":
            cmd = 7005
            req = {
                'Cookie': '12345',
                "LocalID":str(order_id),
                "OrderID":"0",
                'Price': str(int(math.floor(price * 1000))),
                'Qty': str(amount),
                'EnvType': str(envtype),
            }

        data = self.__socket_call(cmd, req)
        return data

    def unlock(self, passcode):
        req = {
            'Cookie': '12345',
            "Password": str(passcode)
        }
        data = self.__socket_call(6006, req)
        return data

    def get_account_info(self, market, envtype=0):
        if market == "hk":
            cmd = 6007
        elif market == "us":
            cmd = 7007
        req = {
            "Cookie": "12345",
            "EnvType": str(envtype)
        }
        data = self.__socket_call(cmd, req)
        for i in data:
            if i.lower() <> 'EnvType'.lower() and i.lower() <> 'Cookie'.lower():
                data[i] = float(data[i]) / 1000
        return data

    def get_order_list(self, market, stockcode=None, envtype=0):
        if market == "hk":
            cmd = 6008
        elif market == "us":
            cmd = 7008
        req = {
            "Cookie": "12345",
            "EnvType": str(envtype)
        }
        data = self.__socket_call(cmd, req)

        # change price
        if market == "hk":
            order_list = data['HKOrderArr']
        elif market == "us":
            order_list = data['USOrderArr']
        if order_list <> None:
            for i in order_list:
                i['Price'] = str(float(i['Price']) / 1000).decode('UTF-8')
            # filter stock code
            if stockcode <> None:
                i = 0
                while i < len(order_list):
                    if cmp_stockcode(order_list[i]['StockCode'], stockcode) <> True:
                        del order_list[i]
                        i = i - 1
                    i = i + 1
        return data


    def get_stock_info(self, market, stockcode=None, envtype=0):
        if market == "hk":
            cmd = 6009
        elif market == "us":
            cmd = 7009
        req = {
            "Cookie": "12345",
            "EnvType": str(envtype)
        }
        data = self.__socket_call(cmd, req)
        if market == "hk":
            stocks_info = data['HKPositionArr']
        elif market == "us":
            stocks_info = data['USPositionArr']
        if stocks_info <> None:
            for stock in stocks_info:
                for i in (u'NominalPrice', 'MarketVal', 'CostPrice', 'PLVal',
                          'PLRatio', 'Today_PLVal', 'Today_BuyVal', 'Today_SellVal'):
                    stock[i] = round(float(stock[i]) / 1000, 3)
             # filter stock code
            if stockcode <> None:
                i = 0
                while i < len(stocks_info):
                    if cmp_stockcode(stocks_info[i]['StockCode'], stockcode) <> True:
                        print "del"
                        del stocks_info[i]
                        i = i - 1
                    i = i + 1
        return data

if __name__ == '__main__':
    futu = Futu()
    #print futu.unlock("123123")
    print futu.get_ticker("hk","00700")
    #print futu.get_ticker("us","CMCM")
    print futu.get_ticker("sh","600036")

    # print futu.get_depth("hk", "00700")
    # print futu.get_depth("us", "CMCM")

    # r =  futu.buy("hk","00700",148.1,100,0,1)
    #print r
    # print futu.modify("hk",int(r['OrderID']),148.0,100,0,1)
    # print futu.cancel("hk",int(r['OrderID']),0,1)

    #r = futu.buy("us", "CMCM", 13.60, 1000)
    #print r
    #print futu.modify("us",int(r['LocalID']),13.40, 1000)
    #print futu.cancel("us",int(r['LocalID']),0)

    # print futu.sell("hk","00700",140.1,100,0,1)
    # print futu.sell("us", "CMCM", 20.60, 1000)

    # print futu.get_order_list("hk","00700")
    # print futu.get_order_list("us","CMCM")
    # print futu.get_account_info("hk")
    # print futu.get_stock_info("hk","00700")
    # print futu.get_stock_info("us","CMCM")
