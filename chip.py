#coding=utf-8
import os
import sys
import datetime
import matplotlib
import const as ct
import numpy as np
import pandas as pd
from log import getLogger
from cstock import CStock
from cmysql import CMySQL
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from common import df_empty, create_redis_obj, get_chinese_font
logger = getLogger(__name__)

def delta_days(pre_pos, pos):
    return pos - pre_pos

class Chip:
    def __init__(self):
        self.redis = create_redis_obj()
        #self.mysql_client = CMySQL(self.dbinfo)

    def get(self, code):
        return CStock(code, should_create_mysqldb = False).get_k_data()

    def average_volume(self, df, pre_outstanding, outstanding):
        if pre_outstanding != outstanding:
            length = len(df)
            if pre_outstanding > outstanding:
                delta_volume = int((pre_outstanding - outstanding) / length)
                df.volume -= delta_volume
                if length * delta_volume < pre_outstanding - outstanding:
                    delta = pre_outstanding - outstanding - length * delta_volume
                    max_index = df.volume.idxmax()
                    df.at[max_index, 'volume'] = df.loc[max_index, 'volume'] - delta
            else:
                delta_volume = int((outstanding - pre_outstanding) / length)
                df.volume += delta_volume
                if length * delta_volume < outstanding - pre_outstanding:
                    delta = outstanding - pre_outstanding - length * delta_volume
                    min_index = df.volume.idxmin()
                    df.at[min_index, 'volume'] = df.loc[min_index, 'volume'] + delta
        return df

    def change_volume(self, df, volume_total):
        while volume_total > 0:
            aver_volume = int(volume_total / len(df))
            if aver_volume > 0:
                for _index, volume in df.volume.iteritems():
                    if volume != 0:
                        t_volume = max(volume - aver_volume, 0)
                        df.at[_index, 'volume'] = t_volume
                        volume_total -= min(aver_volume, volume)
            else:
                max_index = df.volume.idxmax()
                df.at[max_index, 'volume'] = df.loc[max_index, 'volume'] - volume_total
                volume_total = 0
            logger.debug("volume_total:%s, aver_volume:%s, df:%s" % (volume_total, aver_volume, df))
        return df

    def adjust_short_volume(self, s_df, s_volume):
        s_df = s_df.sort_values(by = 'price', ascending= False)
        up_index_list = list()
        s_volume_sum = s_df.volume.sum()
        cur_volume_sum = 0
        for _index, volume in s_df.volume.iteritems():
            cur_volume_sum += volume
            up_index_list.append(_index)
            if float(cur_volume_sum) > 0.5 * s_volume_sum: 
                break

        s_up_df = s_df.loc[up_index_list]
        s_down_df = s_df.loc[~s_df.index.isin(up_index_list)]

        if not s_down_df.empty:
            up_volume = int(s_volume * cur_volume_sum / s_volume_sum)
            if cur_volume_sum > up_volume:
                delta = int(0.1 * (cur_volume_sum - up_volume))
                down_volume = max(0, s_volume - up_volume - delta)
                up_volume = s_volume - down_volume
            else:
                down_volume = s_volume - up_volume

            if s_up_df.volume.sum() < up_volume:
                logger.error("enter s_up_df s_up_df.volume.sum:%s, up_volume:%s" % (s_up_df.volume.sum(), up_volume))
                import pdb
                pdb.set_trace()
            s_up_df = self.change_volume(s_up_df, up_volume)

            if s_down_df.volume.sum() < down_volume:
                logger.error("enter s_down_df s_down_df.volume.sum:%s, down_volume:%s" % (s_down_df.volume.sum(), down_volume))
                import pdb
                pdb.set_trace()
            s_down_df = self.change_volume(s_down_df, down_volume)
            return s_up_df.append(s_down_df)
        else:
            up_volume = s_volume
            s_up_df = self.change_volume(s_up_df, up_volume)
            return s_up_df

    def adjust_volume(self, df, pos, volume, pre_outstanding, outstanding):
        df = self.average_volume(df, pre_outstanding, outstanding)

        #short chip data
        s_df = df[df.apply(lambda df: delta_days(df['pos'], pos), axis=1) <= 60]
        
        #very long chip data
        #l_df = df[(df.apply(lambda df: delta_days(df['pos'], pos), axis=1) > 60) & (df.apply(lambda df: delta_days(df['pos'], pos), axis=1) <= 560)]
        l_df = df[df.apply(lambda df: delta_days(df['pos'], pos), axis=1) > 60]

        s_volume_total = s_df.volume.sum()
        l_volume_total = 0 if l_df.empty else l_df.volume.sum()

        if s_volume_total > volume:
            if 0 == l_volume_total:
                l_volume = 0
            else:
                if int(l_volume_total / 888) > volume:
                    l_volume = int(0.38 * volume)
                else:
                    l_volume = int(l_volume_total / 888)
            s_volume = volume - l_volume
        else:
            logger.info("should debug date")
            s_volume = int(volume * s_volume_total / df.volume.sum())
            l_volume = volume - s_volume

        #change volume rate
        if s_volume_total < s_volume:
            import pdb
            pdb.set_trace()
        s_df = self.adjust_short_volume(s_df, s_volume)

        if l_volume_total < l_volume:
            import pdb
            pdb.set_trace()
        l_df = self.change_volume(l_df, l_volume)

        return s_df if l_df.empty else s_df.append(l_df)

    def distribution(self, data):
        mdtypes = ['int', 'str', 'str', 'float', 'int', 'int']
        mcolumns = ['pos', 'sdate', 'date', 'price', 'volume', 'outstanding']
        df = df_empty(columns = mcolumns, dtypes = mdtypes)
        tmp_df = df_empty(columns = mcolumns, dtypes = mdtypes)
        for _index, cdate in data.cdate.iteritems():
            volume = int(data.loc[_index, 'volume'])
            aprice = data.loc[_index, 'aprice']
            outstanding = data.loc[_index, 'outstanding']
            if 0 == _index:
                open_price = data.loc[_index, 'open'] 
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, aprice, volume, outstanding]], columns = mcolumns))
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, open_price, outstanding - volume, outstanding]], columns = mcolumns))
                tmp_df = tmp_df.reset_index(drop = True)
            else:
                logger.info("index:%s, cdate:%s, sum:%s, pre_outstanding:%s, outstanding:%s, length:%s" % (_index, cdate, tmp_df.volume.sum(), pre_outstanding, outstanding, len(tmp_df)))
                #print('----------------------------S')
                #print(tmp_df)
                #print('----------------------------E')
                #if _index == 561:
                #    import pdb
                #    pdb.set_trace()
                tmp_df = tmp_df.sort_values(by = 'pos', ascending= True)
                tmp_df = self.adjust_volume(tmp_df, _index, volume, pre_outstanding, outstanding)
                #print('++++++++++++++++++++++++++++S')
                #print(tmp_df)
                #print('++++++++++++++++++++++++++++E')
                tmp_df.date = cdate
                tmp_df.outstanding = outstanding
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, aprice, volume, outstanding]], columns = mcolumns))
                tmp_df = tmp_df[tmp_df.volume != 0]
                tmp_df = tmp_df.reset_index(drop = True)
                print('============================S')
                print(tmp_df)
                print('============================E')
                if tmp_df.volume.sum() != outstanding:
                    import pdb
                    pdb.set_trace()
            pre_outstanding = outstanding
            df = df.append(tmp_df)
            df = df[df.volume != 0]
            df = df.reset_index(drop = True)
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
        data = cu.get('000001')
        data = data.reindex(index=data.index[::-1])
        data = data.reset_index(drop = True)
        data['low']    = data['adj'] * data['low']
        data['open']   = data['adj'] * data['open']
        data['high']   = data['adj'] * data['high']
        data['close']  = data['adj'] * data['close']
        data['volume'] = data['volume'].astype(int)
        data['aprice'] = data['adj'] * data['amount'] / data['volume']
        data['totals'] = data['totals'] * 10000
        data['totals'] = data['totals'].astype(int)
        data['outstanding'] = data['outstanding'] * 10000
        data['outstanding'] = data['outstanding'].astype(int)
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
            data = pd.read_json(f.read(), orient='records', lines=True, dtype = {'volume' : int, 'totals': int, 'outstanding': int})

        data = data.sort_values(by = 'cdate', ascending= True)
        data = data.reset_index(drop = True)
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
