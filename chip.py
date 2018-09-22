#coding=utf-8
import os
import matplotlib
import const as ct
import pandas as pd
from log import getLogger
from cstock import CStock
from cmysql import CMySQL
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from common import create_redis_obj
logger = getLogger(__name__)


def get_chinese_font():
    #return FontProperties(fname='/conf/fonts/PingFang.ttc')
    return FontProperties(fname='/Volumes/data/quant/stock/conf/fonts/PingFang.ttc')

class ChipUtility:
    def __init__(self):
        self.redis = create_redis_obj()

    def get(self, code):
        obj = CStock(code, should_create_mysqldb = False)
        return obj.get_k_data()

    def distribution(self):
        pass
    
    def mac(self, perieds):
        pass
    
    def volume_up(self):
        pass
    
    def anti_trend_up(self):
        pass

    def huge_down_in_trend(self):
        pass
    
    def profit_ratio(self):
        pass
    
    def big_raise_without_volume(self):
        pass
    
    def bull_arrangement(self):
        pass
    
    def bear_arrangement(self):
        pass

if __name__ == '__main__':
    if not os.path.exists('data.json'):
        cu = ChipUtility()
        data = cu.get('601318')
        data = data.reindex(index=data.index[::-1])
        data = data.reset_index(drop = True)
        data['low']    = data['adj'] * data['low']
        data['open']   = data['adj'] * data['open']
        data['high']   = data['adj'] * data['high']
        data['close']  = data['adj'] * data['close']
        data['aprice'] = data['amount'] / data['volume']
        bprice = 0
        ulist = list()
        tdays = 0
        for index, price in data['aprice'].iteritems():
            tdays += 1
            bprice = bprice + price
            ulist.append(bprice / tdays)
        data['uprice'] = ulist

        data['8price'] = data.aprice.rolling(8).mean()
        tdays = 0
        bprice = 0
        for index in range(7):
            tdays += 1
            bprice = bprice + float(data.loc[index, 'aprice'])
            data.at[index, '8price'] = bprice / tdays

        data['24price'] = data.aprice.rolling(24).mean()
        tdays = 0
        bprice = 0
        for index in range(23):
            tdays += 1
            bprice = bprice + float(data.loc[index, 'aprice'])
            data.at[index, '24price'] = bprice / tdays

        data['60price'] = data.aprice.rolling(60).mean()
        tdays = 0
        bprice = 0
        for index in range(59):
            tdays += 1
            bprice = bprice + float(data.loc[index, 'aprice'])
            data.at[index, '60price'] = bprice / tdays

        with open('data.json', 'w') as f:
            f.write(data.to_json(orient='records', lines=True))

    else:
        with open('data.json', 'r') as f:
            data = pd.read_json(f.read(), orient='records', lines=True)

        x = data.cdate.tolist()
        xn = range(len(x))
        y = data.close.tolist()
        plt.plot(xn, y, label = "中国平安", linewidth = 1.5)
        plt.xticks(xn, x)
        plt.xlabel('时间', fontproperties = get_chinese_font())
        plt.ylabel('价格', fontproperties = get_chinese_font())
        plt.title('股价变化', fontproperties = get_chinese_font())
        plt.legend(loc = 'upper right', prop = get_chinese_font())
        plt.show()
