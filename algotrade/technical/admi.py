#coding=utf-8
import pandas as pd
def admi(df, n, n_adx = 14):
    """calculate the average directional movement index for given data.
    :param df: pandas.DataFrame
    :param n:
    :param n_adx:
    :return: pandas.DataFrame
    """
    i = 0
    up_index = []
    do_index = []
    while i + 1 <= df.index[-1]:
        up_move = df.loc[i + 1, 'high'] - df.loc[i, 'high']
        do_move = df.loc[i, 'low'] - df.loc[i + 1, 'low']
        if up_move > do_move and up_move > 0:
            up_d = up_move
        else:
            up_d = 0
        up_index.append(up_d)
        if do_move > up_move and do_move > 0:
            do_d = do_move
        else:
            do_d = 0
        do_index.append(do_d)
        i = i + 1
    i = 0
    tr_l = [0]
    while i < df.index[-1]:
        tr = max(df.loc[i + 1, 'high'], df.loc[i, 'close']) - min(df.loc[i + 1, 'low'], df.loc[i, 'close'])
        tr_l.append(tr)
        i = i + 1
    tr_s = pd.Series(tr_l)
    atr = pd.Series(tr_s.ewm(span=n, min_periods=n).mean())
    up_index = pd.Series(up_index)
    do_index = pd.Series(do_index)
    pos_di = pd.Series(up_index.ewm(span=n, min_periods=n).mean() / atr)
    neg_di = pd.Series(do_index.ewm(span=n, min_periods=n).mean() / atr)
    adx = pd.Series((abs(pos_di - neg_di) / (pos_di + neg_di)).ewm(span=n_adx, min_periods=n_adx).mean(), name='adx_' + str(n) + '_' + str(n_adx))
    df = df.join(adx)
    return df
