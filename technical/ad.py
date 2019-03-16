#coding=utf-8
import pandas as pd
def ad(df, n):
    """calculate accumulation/distribution for given data.
    :param df: pandas.dataFrame
    :param n: 
    :return: pandas.dataFrame
    """
    ad = ((2 * df['close'] - df['high'] - df['low']) * df['volume']) / (df['high'] - df['low'])
    roc = ad.diff(n - 1) / ad.shift(n - 1)
    ad = pd.Series(roc, name = 'ad_%s' % n)
    df = df.join(ad)
    return df
