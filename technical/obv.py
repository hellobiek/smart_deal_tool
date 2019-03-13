#coding=utf-8
import pandas as pd
def obv(df, n):
    """calculate on balance volume for given data.
    :param df: pandas.DataFrame
    :param n: 
    :return: pandas.DataFrame
    """
    i = 0
    obv = [0]
    while i < df.index[-1]:
        if df.loc[i + 1, 'close'] - df.loc[i, 'close'] > 0:
            obv.append(df.loc[i + 1, 'volume'])
        if df.loc[i + 1, 'close'] - df.loc[i, 'close'] == 0:
            obv.append(0)
        if df.loc[i + 1, 'close'] - df.loc[i, 'close'] < 0:
            obv.append(-df.loc[i + 1, 'volume'])
        i = i + 1
    obv = pd.Series(obv)
    obv_ma = pd.Series(obv.rolling(n, min_periods=n).mean(), name='obv_' + str(n))
    df = df.join(obv_ma)
    return df
