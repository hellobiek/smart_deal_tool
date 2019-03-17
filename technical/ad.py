#coding=utf-8
import pandas as pd
def ad(df, n):
    """calculate accumulation/distribution for given data.
    :param df: pandas.dataFrame
    :param n: 
    :return: pandas.dataFrame
    """
    clv = ((2 * df['close'] - df['high'] - df['low']) * df['volume']) / (df['high'] - df['low'])
    clv = clv.fillna(0.0) # float division by zero
    clv = clv.ewm(com = n, adjust = True).mean()
    return df.join(pd.Series(clv, name = 'ad_%s' % n))
