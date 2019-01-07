#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct 
from algotrade.selecters.selecter import Selecter
class StrongerThanMarketSelecter(Selecter):
    def __init__(self, dbinfo = ct.DB_INFO, redis_host = None):
        code = ct.SELECTERS_DICT[self.__class__.__name__]
        Selecter.__init__(self, code, dbinfo, redis_host)

    def get_pchange(self, df):
        df = df.sort_values(by = 'date', ascending= True)
        close_price = df['close'].tolist()[-1]
        open_price  = df['open'].tolist()[0]
        return 100 * (close_price - open_price) / open_price

    def choose(self, stock_df, av_df, strength = 10):
        code_list = list()
        av_pchange = self.get_pchange(av_df)
        for code, df in stock_df.groupby('code'):
            code_pchange = self.get_pchange(df)
            if code_pchange > av_pchange + strength:
                code_list.append(code)
        return code_list
