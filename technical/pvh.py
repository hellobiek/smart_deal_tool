def pvh(df, p_threshold, n_threshold):
    '''
        select stock which profit chips is down, and neighbor chips is very small
        input:
            df: dataframe for stock info
            p_threshold: low threshold for profit_chips_rate (percent)
            n_threshold: high threshold for neighbor_chips_rate (percent)
        output:
            list:[(code, date, profit_chips_rate, neighbor_chips_rate)]
    '''
    return list()
