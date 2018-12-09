# cython: language_level=3
import numpy as np
def get_effective_breakup_index(break_index_lists, long num, df):
    cdef long break_index = 0, now_price = 0
    cdef float pre_price = 0.0
    effective_breakup_index_list = list()
    while break_index < len(break_index_lists):
        pre_price = df.at[break_index_lists[break_index], 'uprice']
        if break_index < len(break_index_lists) - 1:
            now_price = df.at[break_index_lists[break_index + 1],'uprice']
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if break_index_lists[break_index + 1] - break_index_lists[break_index] > num:
                    if df.at[break_index_lists[break_index + 1], 'breakup'] * df.at[break_index_lists[break_index], 'breakup'] > 0: raise("get error ip failed")
                    effective_breakup_index_list.append(break_index_lists[break_index])
        else:
            now_price = df.at[len(df) - 1, 'uprice']
            if now_price > pre_price * 1.2 or now_price < pre_price * 0.8:
                effective_breakup_index_list.append(break_index_lists[break_index])
            else:
                if len(df) - break_index_lists[break_index] > num:
                    effective_breakup_index_list.append(break_index_lists[break_index])
        break_index += 1
    #ibase means inex of base(effective break up points)
    df['ibase'] = 0
    cdef long pre_base_index = 0
    for _index, ibase in df.ibase.iteritems():
        if _index != 0 and _index in effective_breakup_index_list: pre_base_index = _index
        df.at[_index, 'ibase'] = pre_base_index
    return effective_breakup_index_list

def get_breakup_data(df):
    df['pos'] = 0
    df.at[df.close > df.uprice, 'pos'] = 1
    df.at[df.close < df.uprice, 'pos'] = -1
    df['pre_pos'] = df['pos'].shift(1)
    df.at[0, 'pre_pos'] = 0
    df['pre_pos'] = df['pre_pos'].astype(int)
    df['breakup'] = 0
    df.at[(df.pre_pos <= 0) & (df.pos > 0), 'breakup'] = 1
    df.at[(df.pre_pos >= 0) & (df.pos < 0), 'breakup'] = -1
    #ibreak up means index of break up
    df['ibreakup'] = 0
    cdef long pre_index = 0
    for _index, breakup in df.breakup.iteritems():
        if _index != 0 and breakup != 0: pre_index = _index
        df.at[_index, 'ibreakup'] = pre_index
    return df.drop(['pos', 'pre_pos'], axis=1)

def base_floating_profit(df, long num, mdate = None):
    cdef int direction = 0
    cdef float base, ppchange
    cdef long s_index = 0, e_index = 0
    if mdate is None:
        df = get_breakup_data(df)
        break_index_lists = df.loc[df.breakup != 0].index.tolist()
        effective_breakup_index_list = get_effective_breakup_index(break_index_lists, num, df)
        df['pday'] = 1
        df['base'] = df['close']
        df['ppchange'] = 0
        if len(effective_breakup_index_list) == 0:
            df['profit'] = (df.close - df.uprice) / df.uprice
        else:
            for e_index in effective_breakup_index_list:
                if s_index == e_index:
                    if len(effective_breakup_index_list) == 1:
                        base = df.loc[s_index, 'uprice']
                        direction = df.loc[s_index, 'breakup']
                        ppchange = 1.1 if direction > 0 else 0.9
                        df.at[s_index:, 'base'] = base
                        df.at[s_index:, 'ppchange'] = ppchange
                        df.at[s_index:, 'pday'] = direction * (df.loc[s_index:].index - s_index + 1)
                else:
                    base = df.loc[s_index, 'uprice']
                    direction = df.loc[e_index, 'breakup']
                    ppchange = 1.1 if direction < 0 else 0.9
                    df.at[s_index:e_index - 1, 'base'] = base
                    df.at[s_index:e_index - 1, 'ppchange'] = ppchange
                    df.at[s_index:e_index - 1, 'pday'] = -1 * direction * (df.loc[s_index:e_index - 1].index - s_index + 1)
                    s_index = e_index
                    if e_index == effective_breakup_index_list[-1]:
                        base = df.loc[e_index, 'uprice']
                        direction = df.loc[e_index, 'breakup']
                        ppchange = 1.1 if direction > 0 else 0.9
                        df.at[e_index:, 'base'] = base
                        df.at[e_index:, 'ppchange'] = ppchange
                        df.at[e_index:, 'pday'] = direction * (df.loc[e_index:].index - e_index + 1)
            df['profit'] = (np.log(df.close) - np.log(df.base)).abs() / np.log(df.ppchange)
    return df.drop(['ppchange'], axis=1)
