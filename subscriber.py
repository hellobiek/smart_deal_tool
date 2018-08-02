#-*-coding:utf-8-*-
import futuquant
#from futuquant.quote.quote_response_handler import *
from futuquant.common.constant import SubType
class Subscriber:
    def __init__(self, host, port):
        self.quote_ctx = futuquant.OpenQuoteContext(host, port)
        self._status = False

    def __del__(self):
        self.quote_ctx.stop()
        self.quote_ctx.close()

    def start(self):
        self.quote_ctx.start()
        self._status = True

    def stop(self):
        self.quote_ctx.stop()
        self._status = False

    def status(self):
        return self._status

    def subscribe_tick(self, code, callback):
        '''订阅一只股票的实时行情数据，接收推送 #设置监听-->订阅-->调用逐笔'''
        self.quote_ctx.set_handler(callback)
        self.quote_ctx.subscribe(code, SubType.TICKER)
        ret_code_rt_ticker, ret_data_rt_ticker = self.quote_ctx.get_rt_ticker(code)
        return ret_code_rt_ticker

if __name__ =='__main__':
    code = 'SH.601318'
