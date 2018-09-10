# coding: utf-8
import numpy as np
from pandas import Series
from collections import Iterable
def hurst(ts):
    if not isinstance(ts, Iterable): return
    n_min, n_max = 2, len(ts)//3
    rs_list = []
    for cut in range(n_min, n_max):
        children = len(ts) // cut
        children_list = [ts[i*children:(i+1)*children] for i in range(cut)]
        l = []
        for a_children in children_list:
            Ma = np.mean(a_children)
            Xta = Series(map(lambda x: x-Ma, a_children)).cumsum()
            Ra = max(Xta) - min(Xta)
            Sa = np.std(a_children)
            rs = Ra / Sa
            l.append(rs)
        rs_list.append(np.mean(l))
    return np.polyfit(np.log(range(2+len(rs_list),2,-1)), np.log(rs_list), 1)[0]
