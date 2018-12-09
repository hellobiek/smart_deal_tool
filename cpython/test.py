#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import line_profiler
from pandas import read_csv
from cchip import compute_distribution
if __name__ == '__main__':
    from cindex import CIndex
    from cstock import CStock
    cdate = None
    cstock = CStock('601318')
    index_info = CIndex('000001').get_k_data(cdate)
    bonus_info = read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    quantity_change_info, price_change_info = cstock.collect_right_info(bonus_info)

    df, _ = cstock.read()

    #modify price and quanity for all split-adjusted share prices
    df = cstock.adjust_share(df, quantity_change_info)
    df = cstock.qfq(df, price_change_info)

    #transfer data to split-adjusted share prices
    df = cstock.transfer2adjusted(df)

    #compute strength relative index
    df = cstock.relative_index_strength(df, index_info)

    #prof = line_profiler.LineProfiler(compute_distribution)
    #prof.enable()  # 开始性能分析
    compute_distribution(df)

    #prof.disable()  # 停止性能分析
    #prof.print_stats(sys.stdout)

    #import cProfile
    #import re
    #cProfile.run('re.compile("chip_client.compute_distribution(df)")')

    #from line_profiler import LineProfiler
    #lp = LineProfiler()
    #lp_wrapper = lp(chip_client.compute_distribution)
    #lp_wrapper(df)
    #lp.print_stats()
