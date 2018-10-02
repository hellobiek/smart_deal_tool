#coding=utf-8
import const as ct
import pandas as pd
from log import getLogger
from common import df_empty, number_of_days
logger = getLogger(__name__)

class Chip:
    def evenly_distributed_new_chips(self, df, pre_outstanding, outstanding):
        if pre_outstanding != outstanding:
            volume_sum = df['volume'].sum()
            if pre_outstanding > outstanding:
                volume_total = pre_outstanding - outstanding
                df['volume'] = df['volume'] - volume_total * df['volume'] / volume_sum
                df['volume'] = df['volume'].astype(int)
                actaul_sum = volume_sum - df['volume'].sum()
                delta_sum = volume_total - actaul_sum
                if delta_sum != 0:
                    delta = 1 if delta_sum < 0 else -1
                    index_list = df.nlargest(abs(delta_sum), 'volume').index.tolist()
                    df.at[df.index.isin(index_list), 'volume'] = df.loc[df.index.isin(index_list), 'volume'] + delta
            else:
                volume_total = outstanding - pre_outstanding
                df['volume'] = df['volume'] + volume_total * df['volume'] / volume_sum
                df['volume'] = df['volume'].astype(int)
                actaul_sum = df['volume'].sum() - volume_sum
                delta_sum = volume_total - actaul_sum
                if delta_sum != 0:
                    delta = -1 if delta_sum < 0 else 1
                    index_list = df.nlargest(abs(delta_sum), 'volume').index.tolist()
                    df.at[df.index.isin(index_list), 'volume'] = df.loc[df.index.isin(index_list), 'volume'] + delta
        return df

    def average_distribute(self, df, volume_total):
        volume_sum = df['volume'].sum()
        df['volume'] = df['volume'] - volume_total * df['volume'] / volume_sum
        df['volume'] = df['volume'].astype(int)
        actaul_sum = volume_sum - df['volume'].sum()
        delta_sum = volume_total - actaul_sum
        if delta_sum != 0:
            delta = 1 if delta_sum < 0 else -1
            index_list = df.nlargest(abs(delta_sum), 'volume').index.tolist()
            df.at[df.index.isin(index_list), 'volume'] = df.loc[df.index.isin(index_list), 'volume'] + delta
        return df

    def change_volume(self, df, volume, price):
        profit_df = df.loc[df.price < price]
        unprofit_df = df.loc[df.price >= price]
        if profit_df.empty or unprofit_df.empty:
            return self.average_distribute(df, volume)

        total_volume = df.volume.sum()
        u_total_volume = unprofit_df.volume.sum()
        u_volume = int(u_total_volume * volume/total_volume)
        p_volume = volume - u_volume
        # give p volume more priority
        if u_volume * 0.2 < total_volume - u_total_volume - p_volume:
            u_volume = int(0.8 * u_volume)
            p_volume = volume - u_volume

        if profit_df.volume.sum() < p_volume: raise Exception("profit_df.volume.sum() is less than p_volume")
        profit_df = self.average_distribute(profit_df, p_volume)

        if unprofit_df.volume.sum() < u_volume: raise Exception("unprofit_df.volume.sum() is less than u_volume")
        unprofit_df = self.average_distribute(unprofit_df, u_volume)
        return profit_df.append(unprofit_df)

    def get_profit_volume(self, df, price):
        profit_df = df.loc[df.price < price]
        return profit_df.volume.sum() if len(profit_df) > 0 else 0

    def adjust_volume(self, df, pos, volume, price, pre_outstanding, outstanding):
        df = self.evenly_distributed_new_chips(df, pre_outstanding, outstanding)

        #short chip data
        s_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) <= 60]
        
        #very long chip data
        l_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) > 60]

        if l_df.empty: return self.change_volume(s_df, volume, price)

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
        expect_delta_s_volume = int((volume - s_volume) * 0.2)
        existed_delta_s_volume = s_volume_total - s_volume
        if existed_delta_s_volume > expect_delta_s_volume:
            s_volume += expect_delta_s_volume

        l_volume = volume - s_volume

        #change short volume rate
        if s_volume_total < s_volume: raise Exception("s_volume_total is less than s_volume")
        s_df = self.change_volume(s_df, s_volume, price)

        #change long volume rate
        if l_volume_total < l_volume: raise Exception("l_volume_total is less than l_volume")
        l_df = self.change_volume(l_df, l_volume, price)
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
                if tmp_df.volume.sum() != outstanding:
                    raise Exception("tmp_df.volume.sum() is not equal to outstanding")
            pre_outstanding = outstanding
            df = df.append(tmp_df)
            df = df[df.volume != 0]
            df = df.reset_index(drop = True)
        return df
