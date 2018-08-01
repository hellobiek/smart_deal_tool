#-*-coding:utf-8-*-
import futuquant
from futuquant.quote.quote_response_handler import *
from futuquant.common.constant import *
class QoutationAsynPush(object):
    def __init__(self):
        self.quote_ctx = futuquant.OpenQuoteContext(host='host.docker.internal', port=11111)
        self.quote_ctx.start()

    def __del__(self):
        self.quote_ctx.stop()
        self.quote_ctx.close()

    def aStockQoutation(self, code):
        '''订阅一只股票的实时行情数据，接收推送'''
        #设置监听-->订阅-->调用逐笔
        self.quote_ctx.set_handler(TickerTest())
        self.quote_ctx.subscribe(code, SubType.TICKER)
        ret_code_rt_ticker, ret_data_rt_ticker = self.quote_ctx.get_rt_ticker(code)
        return ret_code_rt_ticker

class TickerTest(TickerHandlerBase):
    '''获取逐笔 get_rt_ticker 和 TickerHandlerBase'''
    def on_recv_rsp(self, rsp_pb):
        ret_code, ret_data = super(TickerTest, self).on_recv_rsp(rsp_pb)
        return RET_OK, ret_data

if __name__ =='__main__':
    ta = QoutationAsynPush()
    self.aStockQoutation(code)
