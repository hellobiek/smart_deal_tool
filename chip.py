#coding=utf-8
import os
import datetime
import matplotlib
import const as ct
import pandas as pd
from log import getLogger
from cstock import CStock
from cmysql import CMySQL
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from common import create_redis_obj, delta_days
logger = getLogger(__name__)

def get_chinese_font():
    return FontProperties(fname='/Volumes/data/quant/stock/conf/fonts/PingFang.ttc')

class Chip:
    def __init__(self):
        self.redis = create_redis_obj()

    def get(self, code):
        obj = CStock(code, should_create_mysqldb = False)
        return obj.get_k_data()

    def average_volume(self, df, today, pre_outstanding, outstanding):
        length = len(df)
        if pre_outstanding > outstanding:
            delta_volume = (pre_outstanding - outstanding) / length
            delta_volume = int(delta_volume)
            df.volume += delta_volume
            if length * delta_volume < pre_outstanding - outstanding:
                delta = pre_outstanding - outstanding - length * delta_volume
                df.at[0, volume] += delta
        elif pre_outstanding < outstanding:
            delta_volume = (outstanding - pre_outstanding) / length
            delta_volume = int(delta_volume)
            df.volume += delta_volume
            if length * delta_volume < outstanding - pre_outstanding:
                delta = outstanding - pre_outstanding - length * delta_volume
                df.at[0, volume] += delta
        return df

    def change_volume(self, df, volume_total):
        while volume_total > 0:
            aver_volume = int(volume_total/len(df))
            if aver_volume > 0:
                for _index, volume in df.volume.iteritems():
                    if volume != 0:
                        t_volume = max(volume - aver_volume, 0)
                        df.at[_index, 'volume'] = t_volume
                        volume_total -= t_volume
            else:
                df.at[_index, 'volume'] -= volume_total
                volume_total = 0
        return df

    def adjust_short_volume(self, s_df, volume):
        s_df = s_df.sort_values(by = 'volume', ascending= False)
        volume_sum = s_df.volume.sum()

        cur_volume_sum = 0
        up_index_list = list()
        for _index, volume in s_df.volume.iteritems():
            cur_volume_sum += volume
            if cur_volume_sum / volume_sum > 0.5: 
                index_list.append(_index)
                break

        s_up_df = s_df.loc[s_df.index in up_index_list]
        s_down_df = s_df.loc[s_df.index not in up_index_list]

        if cur_volume_sum > 0.62 * volume:
            dowm_volume = int(0.38 * volume)
            up_volume = volume - dowm_volume
        else:
            up_volume = cur_volume_sum
            dowm_volume = volume - up_volume

        s_up_df = self.change_volume(s_up_df, up_volume)
        s_down_df = self.change_volume(s_down_df, dowm_volume)
        return s_up_df.append(s_down_df)

    def adjust_volume(self, df, today, volume, pre_outstanding, outstanding):
        df = self.average_volume(df, today, pre_outstanding, outstanding)
        s_df = df[df.apply(lambda df: delta_days(df['sdate'], today), axis=1) < 60]
        s_volume_total = s_df.volume.sum()
        #adjust volume in short term peried
        s_df = self.adjust_short_volume(s_df, s_volume_total)

        #adjust volume in long term peried
        delta_volume = volume - s_volume_total if s_volume_total < volume else 0
        l_df = df[(df.apply(lambda df: delta_days(df['sdate'], today), axis=1) > 60) & (df.apply(lambda df: delta_days(df['sdate'], today), axis=1) < 560)]
        l_volume_total = l_df.volume.sum() + delta_volume
        l_df = self.change_volume(l_df, l_volume_total)
        return s_df.append(l_df)

    def distribution(self, data):
        mcolumns = ['sdate', 'edate', 'price', 'volume', 'outstanding']
        df = pd.DataFrame(columns = mcolumns)
        for _index, cdate in data.cdate.iteritems():
            volume = int(data.loc[_index, 'volume'])
            aprice = int(data.loc[_index, 'aprice'])
            outstanding = data.loc[_index, 'outstanding']
            if 0 == _index:
                open_price = data.loc[_index, 'open'] 
                insertRow = pd.DataFrame([[cdate, cdate, aprice, volume, outstanding]], columns = mcolumns)
                df = df.append(insertRow)
                insertRow = pd.DataFrame([[cdate, cdate, open_price, outstanding - volume, outstanding]], columns = mcolumns)
                df = df.append(insertRow)
            else:
                new_df = self.adjust_volume(df.loc[df.edate == pre_date], cdate, volume, pre_outstanding, outstanding)
                insertRow = pd.DataFrame([[cdate, cdate, aprice, volume, outstanding]], columns = mcolumns)
                new_df = new_df.append(insertRow)
                df = df.append(new_df)
            pre_date = cdate
            df = df.reset_index(drop = True)
            pre_outstanding = outstanding
            print(df)
            import sys
            sys.exit(0)
        return df
    
    def volume_up(self):
        pass
    
    def anti_trend_up(self):
        pass
 
    def bull_arrangement(self):
        pass
    
    def bear_arrangement(self):
        pass

    def big_raise_without_volume(self):
        pass

if __name__ == '__main__':
    cu = Chip()
    if not os.path.exists('data.json'):
        data = cu.get('601318')
        data = data.reindex(index=data.index[::-1])
        data = data.reset_index(drop = True)
        data['low']    = data['adj'] * data['low']
        data['open']   = data['adj'] * data['open']
        data['high']   = data['adj'] * data['high']
        data['close']  = data['adj'] * data['close']
        data['aprice'] = data['adj'] * data['amount'] / data['volume']
        data['volume'] = data['volume'].astype(float)
        data['outstanding'] = data['outstanding'] * 10000
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

        data = data[['cdate', 'open', 'aprice', 'outstanding', 'volume', 'amount']]
        dist = cu.distribution(data)

        #x = data.cdate.tolist()
        #xn = range(len(x))
        #y = data.close.tolist()
        #plt.plot(xn, y, label = "中国平安", linewidth = 1.5)
        #plt.xticks(xn, x)
        #plt.xlabel('时间', fontproperties = get_chinese_font())
        #plt.ylabel('价格', fontproperties = get_chinese_font())
        #plt.title('股价变化', fontproperties = get_chinese_font())
        #plt.legend(loc = 'upper right', prop = get_chinese_font())
        #plt.show()
