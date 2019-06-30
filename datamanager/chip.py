#coding=utf-8
import const as ct
import pandas as pd
from common import df_empty
from base.clog import getLogger
logger = getLogger(__name__)
class Chip:
    def evenly_distributed_new_chip(self, volume_series, pre_outstanding, outstanding):
        volume_series = volume_series * (outstanding / pre_outstanding)
        volume_series = volume_series.astype(int)
        delta_sum = outstanding - volume_series.sum()
        if delta_sum != 0:
            delta = 1 if pre_outstanding < outstanding else -1
            index_list = volume_series.nlargest(delta_sum).index.tolist()
            volume_series[index_list] = volume_series[index_list] + delta
        return volume_series

    def average_distribute(self, df, volume_total):
        volume_sum = df['volume'].sum()
        df['volume'] = df['volume'] * ((volume_sum - volume_total) / volume_sum)
        df['volume'] = df['volume'].astype(int)
        actaul_sum = volume_sum - df['volume'].sum()
        delta_sum = volume_total - actaul_sum
        if delta_sum != 0:
            delta = 1 if delta_sum < 0 else -1
            index_list = df.nlargest(abs(delta_sum), 'volume').index.tolist()
            df.at[df.index.isin(index_list), 'volume'] = df.loc[df.index.isin(index_list), 'volume'] + delta
        return df

    def divide_according_time(self, df, total_volume, now_pos):
        total_holding_time = now_pos * len(df) - df.pos.sum()
        df = df.sort_values(by = 'pos', ascending= True)
        while total_volume != 0:
            for _index, pos in df.pos.iteritems():
                holding_time = now_pos - pos
                expected_volume = max(1, int(total_volume * (holding_time / total_holding_time)))
                if expected_volume > total_volume: expected_volume = total_volume
                total_volume -= min(df.at[_index, 'volume'], expected_volume)
                df.at[_index, 'volume'] = max(0, df.at[_index, 'volume'] - expected_volume)
                if 0 == total_volume: break
        return df

    def divide_according_profit(self, df, total_volume, now_price):
        total_profit = now_price * len(df) - df.price.sum()
        df = df.sort_values(by = 'price', ascending = True)
        while total_volume != 0:
            for _index, price in df.price.iteritems():
                profit = now_price - price
                expected_volume = max(1, int(total_volume * (profit / total_profit)))
                if expected_volume > total_volume: expected_volume = total_volume
                total_volume -= min(df.at[_index, 'volume'], expected_volume)
                df.at[_index, 'volume'] = max(0, df.at[_index, 'volume'] - expected_volume)
                if 0 == total_volume: break
        return df

    def change_volume_for_short(self, df, volume, price, pos):
        profit_df = df.loc[df.price < price]
        unprofit_df = df.loc[df.price >= price]
        if profit_df.empty:
            return self.average_distribute(unprofit_df, volume)

        if unprofit_df.empty:
            return self.divide_according_profit(profit_df, volume, price)

        total_volume = df.volume.sum()
        u_total_volume = unprofit_df.volume.sum()
        u_volume = int(volume * (u_total_volume/total_volume))
        p_volume = volume - u_volume

        if profit_df.volume.sum() < p_volume: raise Exception("profit_df.volume.sum() is less than p_volume")
        #profit_df = self.average_distribute(profit_df, p_volume)
        profit_df = self.divide_according_profit(profit_df, p_volume, price)

        if unprofit_df.volume.sum() < u_volume: raise Exception("unprofit_df.volume.sum() is less than u_volume")
        unprofit_df = self.average_distribute(unprofit_df, u_volume)
        return profit_df.append(unprofit_df)

    def change_volume_for_long(self, df, volume, price, pos):
        profit_df = df.loc[df.price < price]
        unprofit_df = df.loc[df.price >= price]
        if profit_df.empty:
            return self.divide_according_time(unprofit_df, volume, pos)

        if unprofit_df.empty:
            return self.average_distribute(profit_df, volume)

        total_volume = df.volume.sum()
        u_total_volume = unprofit_df.volume.sum()
        u_volume = int(volume * (u_total_volume/total_volume))
        p_volume = volume - u_volume
        #give p volume more priority
        #if u_volume * 0.05 < total_volume - u_total_volume - p_volume:
        #    u_volume = int(0.95 * u_volume)
        #    p_volume = volume - u_volume

        if profit_df.volume.sum() < p_volume: raise Exception("profit_df.volume.sum() is less than p_volume")
        profit_df = self.average_distribute(profit_df, p_volume)
        #profit_df = self.divide_according_profit(profit_df, p_volume, price)

        if unprofit_df.volume.sum() < u_volume: raise Exception("unprofit_df.volume.sum() is less than u_volume")
        #unprofit_df = self.average_distribute(unprofit_df, u_volume)
        unprofit_df = self.divide_according_time(unprofit_df, u_volume, pos)
        return profit_df.append(unprofit_df)

    def get_profit_volume(self, df, price):
        profit_df = df.loc[df.price < price]
        return profit_df.volume.sum() if len(profit_df) > 0 else 0

    def adjust_volume(self, df, pos, volume, price, pre_outstanding, outstanding):
        def number_of_days(pre_pos, pos):
            return pos - pre_pos
        if pre_outstanding != outstanding:
            df['volume'] = self.evenly_distributed_new_chip(df['volume'], pre_outstanding, outstanding)

        #short chip data
        s_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) <= 60]
        
        #very long chip data
        l_df = df[df.apply(lambda df: number_of_days(df['pos'], pos), axis=1) > 60]

        if l_df.empty:
            return self.change_volume_for_short(s_df, volume, price, pos)

        #short term volume
        s_volume_total = s_df.volume.sum()
        #s_p_volume = self.get_profit_volume(s_df, price)
       
        #long term volume
        l_volume_total = l_df.volume.sum()
        #l_p_volume = self.get_profit_volume(l_df, price)

        #total volume
        volume_total = s_volume_total + l_volume_total
        #p_volume_total = s_p_volume + l_p_volume

        ##compute a good short term volume
        #if s_volume_total >= volume and l_volume_total >= volume:
        #    if 0 == p_volume_total:
        #        s_volume = int(volume * (s_volume_total / volume_total))
        #    else:
        #        s_volume = int(volume * (s_p_volume / p_volume_total))
        #elif s_volume_total < volume  and l_volume_total > volume:
        #    if 0 == p_volume_total or s_p_volume * volume >= p_volume_total * s_volume_total:
        #        s_volume = int(volume * (s_volume_total / volume_total))
        #    else:
        #        s_volume = int(volume * (s_p_volume / p_volume_total))
        #elif s_volume_total > volume and l_volume_total < volume:
        #    if 0 == p_volume_total or l_p_volume * volume >= p_volume_total * l_volume_total:
        #        s_volume = int(volume * (s_volume_total / volume_total))
        #    else:
        #        s_volume = int(volume * (s_p_volume / p_volume_total))
        #else:
        #    if 0 == p_volume_total or (s_p_volume * volume >= p_volume_total * s_volume_total and l_p_volume * volume >= p_volume_total * l_volume_total):
        #        s_volume = int(volume * (s_volume_total / volume_total))
        #    else:
        #        s_volume = int(volume * (s_p_volume / p_volume_total))

        s_volume = int(volume * (s_volume_total / volume_total))
        ##give higher priority to short term volume
        #expect_delta_s_volume = int((volume - s_volume) * 0.1)
        #existed_delta_s_volume = s_volume_total - s_volume
        #if existed_delta_s_volume > expect_delta_s_volume:
        #    s_volume += expect_delta_s_volume

        l_volume = volume - s_volume

        #change short volume rate
        if s_volume_total < s_volume: raise Exception("s_volume_total is less than s_volume")
        s_df = self.change_volume_for_short(s_df, s_volume, price, pos)

        #change long volume rate
        if l_volume_total < l_volume: raise Exception("l_volume_total is less than l_volume")
        l_df = self.change_volume_for_long(l_df, l_volume, price, pos)
        return s_df.append(l_df)

    def compute_distribution(self, data):
        mdtypes = ['int', 'str', 'str', 'float', 'int', 'int']
        df = df_empty(columns = ct.CHIP_COLUMNS, dtypes = mdtypes)
        tmp_df = df_empty(columns = ct.CHIP_COLUMNS, dtypes = mdtypes)
        for _index, row in data.iterrows():
            #logger.info("compute %s" % _index)
            cdate, volume, aprice, outstanding = row[['date', 'volume', 'aprice', 'outstanding']]
            if 0 == _index:
                open_price = data.at[_index, 'open']
                list1 = [_index, cdate, cdate, aprice, volume, outstanding]
                list2 = [_index, cdate, cdate, open_price, outstanding - volume, outstanding]
                tmp_df = tmp_df.append(pd.DataFrame([list1, list2], columns = ct.CHIP_COLUMNS))
            else:
                tmp_df = tmp_df.sort_values(by = 'pos', ascending = True)
                tmp_df = self.adjust_volume(tmp_df, _index, volume, aprice, pre_outstanding, outstanding)
                tmp_df.date = cdate
                tmp_df.outstanding = outstanding
                tmp_df = tmp_df.append(pd.DataFrame([[_index, cdate, cdate, aprice, volume, outstanding]], columns = ct.CHIP_COLUMNS))
                tmp_df = tmp_df[tmp_df.volume != 0]
                if tmp_df.volume.sum() != outstanding: raise Exception("tmp_df.volume.sum() is not equal to outstanding")
            tmp_df = tmp_df.reset_index(drop = True)
            pre_outstanding = outstanding
            df = df.append(tmp_df)
            df = df[df.volume != 0]
            df = df.reset_index(drop = True)
        return df

if __name__ == '__main__':
    from cindex import CIndex
    from cstock import CStock
    cdate = None
    cstock = CStock('601318')
    index_info = CIndex('000001').get_k_data(cdate)
    bonus_info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    quantity_change_info, price_change_info = cstock.collect_right_info(bonus_info)

    df, _ = cstock.read()

    #modify price and quanity for all split-adjusted share prices
    df = cstock.adjust_share(df, quantity_change_info)
    df = cstock.qfq(df, price_change_info)

    #transfer data to split-adjusted share prices
    df = cstock.transfer2adjusted(df)

    #compute strength relative index
    df = cstock.relative_index_strength(df, index_info)

    chip_client = Chip()
    chip_client.compute_distribution(df)

    #import cProfile
    #import re
    #cProfile.run('re.compile("chip_client.compute_distribution(df)")')

    from line_profiler import LineProfiler
    lp = LineProfiler()
    lp_wrapper = lp(chip_client.compute_distribution)
    lp_wrapper(df)
    lp.print_stats()
