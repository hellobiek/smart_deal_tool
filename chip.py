#coding=utf-8
import const as ct
import pandas as pd
from log import getLogger
from common import df_empty, number_of_days
logger = getLogger(__name__)

class Chip:
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
                delta = int(0.05 * (cur_volume_sum - up_volume))
                down_volume = max(0, s_volume - up_volume - delta)
                up_volume = s_volume - down_volume
            else:
                down_volume = s_volume - up_volume

            if s_up_df.volume.sum() < up_volume: raise Exception("s_up_df.volume.sum() is less than up_volume")
            s_up_df = self.change_volume(s_up_df, up_volume)

            if s_down_df.volume.sum() < down_volume: raise Exception("s_down_df.volume.sum() is less than down_volume")
            s_down_df = self.change_volume(s_down_df, down_volume)
            return s_up_df.append(s_down_df)
        else:
            up_volume = s_volume
            s_up_df = self.change_volume(s_up_df, up_volume)
            return s_up_df

    def get_profit_volume(self, df, price):
        profit_df = df.loc[df.price < price]
        if len(profit_df) > 0:
            return profit_df.volume.sum()
        else:
            return 0

    def adjust_volume(self, df, pos, volume, price, pre_outstanding, outstanding):
        df = self.average_volume(df, pre_outstanding, outstanding)

        #short chip data
        s_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) <= 60]
        
        #very long chip data
        l_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) > 60]

        if l_df.empty:
            return self.adjust_short_volume(s_df, volume)
        else:
            #short term volume
            s_volume_total = s_df.volume.sum()
            s_p_volume = self.get_profit_volume(s_df, price)
       
            #long term volume
            l_volume_total = l_df.volume.sum()
            l_p_volume = self.get_profit_volume(l_df, price)

            #total volume
            p_volume_total = s_p_volume + l_p_volume
            volume_total = s_volume_total + l_volume_total

            #compute a good short term volume
            if s_volume_total >= volume and l_volume_total >= volume:
                if 0 == p_volume_total:
                    s_volume = int(volume * s_volume_total / volume_total)
                else:
                    s_volume = int(volume * s_p_volume / p_volume_total)
            elif s_volume_total < volume  and l_volume_total > volume:
                if 0 == p_volume_total or s_p_volume * volume >= p_volume_total * s_volume_total:
                    s_volume = int(volume * s_volume_total / volume_total)
                else:
                    s_volume = int(volume * s_p_volume / p_volume_total)
            elif s_volume_total > volume and l_volume_total < volume:
                if 0 == p_volume_total or l_p_volume * volume >= p_volume_total * l_volume_total:
                    s_volume = int(volume * s_volume_total / volume_total)
                else:
                    s_volume = int(volume * s_p_volume / p_volume_total)
            else:
                if 0 == p_volume_total or (s_p_volume * volume >= p_volume_total * s_volume_total and l_p_volume * volume >= p_volume_total * l_volume_total):
                    s_volume = int(volume * s_volume_total / volume_total)
                else:
                    s_volume = int(volume * s_p_volume / p_volume_total)

            #give higher priority to short term volume
            expect_delta_s_volume = int((volume - s_volume) * 0.05)
            existed_delta_s_volume = s_volume_total - s_volume
            if existed_delta_s_volume > expect_delta_s_volume:
                s_volume += expect_delta_s_volume

            l_volume = volume - s_volume

            #change short volume rate
            if s_volume_total < s_volume: raise Exception("s_volume_total is less than s_volume")
            s_df = self.change_volume(s_df, s_volume)
            #s_df = self.adjust_short_volume(s_df, s_volume)

            #change long volume rate
            if l_volume_total < l_volume: raise Exception("l_volume_total is less than l_volume")
            l_df = self.change_volume(l_df, l_volume)
            #l_df = self.adjust_short_volume(l_df, l_volume)

            return s_df.append(l_df)

    def compute_distribution(self, data):
        mdtypes = ['int', 'str', 'str', 'float', 'int', 'int']
        df = df_empty(columns = ct.CHIP_COLUMNS, dtypes = mdtypes)
        tmp_df = df_empty(columns = ct.CHIP_COLUMNS, dtypes = mdtypes)
        for _index, cdate in data.cdate.iteritems():
            volume = int(data.loc[_index, 'volume'])
            aprice = data.loc[_index, 'aprice']
            outstanding = data.loc[_index, 'outstanding']
            if 0 == _index:
                open_price = data.loc[_index, 'open'] 
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, aprice, volume, outstanding]], columns = ct.CHIP_COLUMNS))
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, open_price, outstanding - volume, outstanding]], columns = ct.CHIP_COLUMNS))
                tmp_df = tmp_df.reset_index(drop = True)
            else:
                tmp_df = tmp_df.sort_values(by = 'pos', ascending= True)
                tmp_df = self.adjust_volume(tmp_df, _index, volume, aprice, pre_outstanding, outstanding)
                tmp_df.date = cdate
                tmp_df.outstanding = outstanding
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, aprice, volume, outstanding]], columns = ct.CHIP_COLUMNS))
                tmp_df = tmp_df[tmp_df.volume != 0]
                tmp_df = tmp_df.reset_index(drop = True)
                if tmp_df.volume.sum() != outstanding: raise Exception("tmp_df.volume.sum() is not equal to outstanding")
            pre_outstanding = outstanding
            df = df.append(tmp_df)
            df = df[df.volume != 0]
            df = df.reset_index(drop = True)
        return df
