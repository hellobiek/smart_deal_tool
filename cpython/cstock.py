def pro_nei_chip(df, dist_data, preday_df = None, mdate = None):
    if mdate is None:
        p_profit_vol_list = list()
        p_neighbor_vol_list = list()
        groups = dist_data.groupby(dist_data.date)
        for _index, cdate in df.date.iteritems():
            drow = df.loc[_index]
            close_price = drow['close']
            outstanding = drow['outstanding']
            group = groups.get_group(cdate)
            p_val = 100 * group[group.price < close_price].volume.sum() / outstanding
            n_val = 100 * group[(group.price < close_price * 1.08) & (group.price > close_price * 0.92)].volume.sum() / outstanding
            p_profit_vol_list.append(p_val)
            p_neighbor_vol_list.append(n_val)
        df['ppercent'] = p_profit_vol_list
        df['npercent'] = p_neighbor_vol_list
        df['gamekline'] = df['ppercent'] - df['ppercent'].shift(1)
        df.at[0, 'gamekline'] = df.loc[0, 'ppercent']
    else:
        p_close = df['close'].values[0]
        outstanding = df['outstanding'].values[0]
        p_val = 100 * dist_data[dist_data.price < p_close].volume.sum() / outstanding
        n_val = 100 * dist_data[(dist_data.price < p_close * 1.08) & (dist_data.price > p_close * 0.92)].volume.sum() / outstanding
        df['ppercent'] = p_val
        df['npercent'] = n_val
        df['gamekline'] = df['ppercent'].values[0] - preday_df['ppercent'].values[0]
    return df
