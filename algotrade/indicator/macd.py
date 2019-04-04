# -*- encoding:utf-8 -*-
import traceback
import numpy as np
import pandas as pd
from enum import Enum
from algotrade.indicator.config import *
"""
  MACD信号检测
  使用注意：先初始化MacdCache, 缓存历史数据
  包含以下几个类：
  - TOSTR                     实例转换成字符串
  - GoldCross                 金叉
  - DeathCross                死叉
  - CrossDetect               金叉死叉检测
  - MaxLimitDetect            最大值检测[相邻的金叉和死叉间的最大值]
  - MinLimitDetect            最小值检测[相邻的死叉和金叉间的最小值]
  - DivergenceType            背离的类型[顶背离、底背离]
  - Divergence                背离
  - DivergenceDetect          背离检测
  - TopDivergenceDetect       顶背离检测
  - BottomDivergenceDetect    底背离检测
  - Indicator                 macd指标检测, 包含：金叉、死叉、极值、背离
  - MacdCache                 缓存数据。
                              检测指标需要用到的数据：收盘价、dif、dea、macd
                              以及已经检测到的指标: 金叉、死叉、极值、背离
                              注意：背离只缓存最近一根bar触发的背离
"""
class TOSTR:
    """将实例转换成字符串，以方便输出到日志显示"""
    def get_attr(self):
        """
        获取实例的所有属性
        :return:
        """
        return self.__dict__.items()

    def to_json(self):
        """
        将实例转换成json字符串的形式。继承的子类实现
        """
        raise NotImplementedError()

    @staticmethod
    def to_json_list(obj_list):
        """
        将包含实例的数组转换成json字符串数组
        :param obj_list:
        :return:
        """
        dl = []
        for obj in obj_list:
            if obj:
                dl.append(obj.to_json())
            else:
                dl.append(None)
        return dl

class GoldCross:
    """
    定义金叉
    """
    def __init__(self):
        self.cross_type = GOLD

    @staticmethod
    def is_cross(pre_macd, macd):
        """
        判断是否金叉
        :param pre_macd: 前一个bar的macd
        :param macd: 当前bar的macd
        :return:
        """
        return pre_macd <= 0 < macd

class DeathCross:
    """
    定义死叉
    """
    def __init__(self):
        self.cross_type = DEATH

    @staticmethod
    def is_cross(pre_macd, macd):
        """
        判断是否死叉
        :param pre_macd: 前一个bar的macd
        :param macd: 当前bar的macd
        :return:
        """
        return pre_macd >= 0 > macd

class CrossDetect:
    """
    检测金叉死叉
    """
    @staticmethod
    def is_cross(df, name = MACD, cross):
        """
        检测最后一根bar是不是定义的交叉类型
        :param df: DataFrame类型。缓存的数据，最后一条记录是待检测的bar
        :param cross: 金叉或死叉
        :return:
        """
        if df.empty or len(df) == 1:
            return False
        row = df.iloc[-1]
        pre_row = df.iloc[-2]
        if not cross.is_cross(pre_row[name], row[name]):
            log.debug('【%s】没有穿过, macd=%s, pre_macd=%s' % (row.name, row[name], pre_row[name]))
            return False
        return True

class MaxLimitDetect:
    """
    检测极值：最大值的时间。用于检测3种极值的时间，3种极值分别是：DIF/CLOSE/MACD
    """
    @classmethod
    def get_close_limit_tm_in(cls, df):
        """
        获取区间内CLOSE最大值对应的时间。
        :param df: DataFrame类型， 相邻的金叉和死叉之间或两个金叉之间的所有数据[包含金叉点，不包含死叉点]
        :return:
        """
        limit = df[CLOSE].max()
        if limit > 0:
            # 必须是一个有效的价格
            return cls.__get_max_limit_tm(df[CLOSE], limit)

    @classmethod
    def get_dif_limit_tm_in(cls, df):
        """
        获取区间内DIF最大值对应的时间。
        :param df: DataFrame类型， 相邻的金叉和死叉之间或两个金叉之间的所有数据[包含金叉点，不包含死叉点]
        :return:
        """
        limit = df[DIF].max()
        if limit > 0:
            # 要求当前区间内的最大值必须在零轴上。
            return cls.__get_max_limit_tm(df[DIF], limit)

    @classmethod
    def get_macd_limit_tm_in(cls, df):
        """
        获取区间内MACD最大值对应的时间。
        :param df: DataFrame类型， 相邻的金叉和死叉之间或两个金叉之间的所有数据[包含金叉点，不包含死叉点]
        :return:
        """
        limit = df[MACD].max()
        if limit > 0:
            # 要求当前区间内的最大值必须是红柱。
            return cls.__get_max_limit_tm(df[MACD], limit)

    @staticmethod
    def __get_max_limit_tm(series, limit):
        """
        获取series连续区间内的最大值所对应的时间。
        不同的数据源，累计的分钟级别close可能会不同，根据close计算的dif、macd也会不同，
        为了降低对于极值点的敏感度, 取所有与limit接近的值作为候选的极值点。
        存在多个候选的极值点时，取离当前交叉点最近的一个作为极值点。
        :param series: 时间序列数据, Series类型。
        :param limit: 当前区间内的最大值
        :return: 最大值的时间
        """
        # 所有候选的极值点
        limits = series[series >= limit * LIMIT_DETECT_LIMIT_FACTOR]
        if not limits.empty:
            tm = limits.index[-1]
            return tm

class MinLimitDetect:
    """
    检测极值：最小值的时间。用于检测3种极值的时间，3种极值分别是：DIF/CLOSE/MACD
    """
    @classmethod
    def get_close_limit_tm_in(cls, df):
        """
        获取区间内close最小值的时间。
        :param df: DataFrame类型， 相邻的死叉和金叉之间或两个死叉之间的所有数据[包含死叉点，不包含金叉点]
        :return:
        """
        limit = df[CLOSE].min()
        if limit > 0:
            # 必须是一个有效的价格。
            limit_df = df[df[CLOSE] <= limit * (2 - LIMIT_DETECT_LIMIT_FACTOR)]
            if not limit_df.empty:
                tm = limit_df.index[-1]
                return tm

    @classmethod
    def get_dif_limit_tm_in(cls, df):
        """
        获取区间内DIF最小值的时间。
        :param df: DataFrame类型， 相邻的死叉和金叉之间或两个死叉之间的所有数据[包含死叉点，不包含金叉点]
        :return:
        """
        limit = df[DIF].min()
        if limit < 0:
            # 要求当前区间内的最小值必须在零轴下。
            return cls.__get_min_limit_tm(df[DIF], limit)

    @classmethod
    def get_macd_limit_tm_in(cls, df):
        """
        获取区间内MACD最大值的时间。
        :param df: DataFrame类型， 相邻的死叉和金叉之间或两个死叉之间的所有数据[包含死叉点，不包含金叉点]
        :return:
        """
        limit = df[MACD].min()
        if limit < 0:
            # 要求当前区间内的最小值必须是绿柱。
            return cls.__get_min_limit_tm(df[MACD], limit)

    @staticmethod
    def __get_min_limit_tm(series, limit):
        """
        获取series连续区间内的最小值的时间。
        不同的数据源，累计的分钟级别close可能会不同，根据close计算的dif、macd也会不同，
        为了降低对于极值点的敏感度, 取所有与limit接近的值作为候选的极值点。
        存在多个候选的极值点时，取离当前交叉点最近的一个作为极值点。
        :param series: 时间序列数据, Series类型。
        :param limit: 当前区间内的最大值
        :return: 最大值的时间
        """
        # 所有候选的极值点
        limits = series[series <= limit * LIMIT_DETECT_LIMIT_FACTOR]
        if not limits.empty:
            tm = limits.index[-1]
            return tm

class DivergenceType(Enum):
    """
    定义背离的类型
    """
    Top = 'TOP'  # 顶背离
    Bottom = 'BOTTOM'  # 底背离

class Divergence(TOSTR):
    """
    背离
    """
    def __init__(self, divergence_type=None, pre_dif_limit_tm=None, last_dif_limit_tm=None, significance=None):
        self.divergence_type = divergence_type
        self.pre_dif_limit_tm = pre_dif_limit_tm
        self.last_dif_limit_tm = last_dif_limit_tm
        self.significance = significance  # 背离的可见度

    def to_json(self):
        return {
            'type': self.divergence_type,
            'pre_dif_limit_tm': str(self.pre_dif_limit_tm),
            'last_dif_limit_tm': str(self.last_dif_limit_tm),
            'significance': self.significance
        }

class DivergenceDetect:
    """
    检测背离
    """
    def __init__(self):
        self.most_limit_num = DIVERGENCE_DETECT_MOST_LIMIT_NUM  # 最大允许在几个相邻的极值点检测背离。
        self.significance = DIVERGENCE_DETECT_SIGNIFICANCE  # 背离的可见度
        self.dif_limit_bar_num = DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM  # bar的数量。往前追溯多少个bar计算dif最大值
        self.dif_limit_factor = DIVERGENCE_DETECT_DIF_LIMIT_FACTOR  # dif极值的调节因子。
        self.cross_type = None  # 交叉点的类型[金叉或死叉]
        self.divergence_type = None  # 背离的类型[顶背离或底背离]

    def is_valid_by_zero_axis(self, dif, pre_dif):
        """
        验证两个极值点的dif与零轴的关系，是否满足背离要求。具体的验证方法由子类实现
        :param dif:
        :param pre_dif:
        :return:
        """
        raise NotImplementedError()

    def is_valid_by_close_and_dif(self, close, pre_close, dif, pre_dif):
        """
        验证两个极值点的dif和close，是否满足背离要求。具体的验证方法由子类实现
        :param close:
        :param pre_close:
        :param dif:
        :param pre_dif:
        :return:
        """
        raise NotImplementedError()

    def get_divergences(self, df):
        """
        检测最近一个bar是否发生背离
        :param df: DataFrame类型，至少包含以下列：CLOSE、DIF、MACD、是否金叉、是否死叉、
                   以及根据交叉点检测到的3种极值的时间，DIF极值时间、MACD极值时间、收盘价极值时间
        :return: 背离
        """
        divergences = []
        row = df.iloc[-1]

        if not self.cross_type or not row[self.cross_type]:
            return divergences
        cdf = df[df[self.cross_type].notnull()]

        cdf = cdf[cdf[self.cross_type]].iloc[-self.most_limit_num:]  # 相邻的N个金叉点
        if len(cdf) <= 1:  # 只找到一个极值点
            log.debug('【%s, %s】只有一个极值点, dif_limit_tm=%s' % (row.name, self.cross_type, row[DIF_LIMIT_TM]))
            return divergences

        dif, close, macd = self.get_limit_by_cross(df, row)
        if dif is None or close is None or macd is None:
            log.debug('【%s, %s】未找到穿越前的极值, dif_limit_tm=%s' % (row.name, self.cross_type, row[DIF_LIMIT_TM]))
            return divergences

        for i in range(len(cdf) - 2, -1, -1):
            pre_cross = cdf.iloc[i]
            pre_dif, pre_close, pre_macd = self.get_limit_by_cross(df, pre_cross)
            if pre_dif is None or pre_close is None or pre_macd is None:
                log.debug('【%s, %s】未找到前一个背离点的极值, dif_limit_tm=%s' % (row.name, self.cross_type, row[DIF_LIMIT_TM]))
                continue

            # 分别对比两个点的价格以及dif的高低关系.顶背离：价格创新高，dif没有创新高，底背离：价格创新低，dif没有创新低
            if not self.is_valid_by_close_and_dif(close[CLOSE], pre_close[CLOSE], dif[DIF], pre_dif[DIF]):
                log.debug(
                        '【%s, %s】极值点价格和DIF分别比较, dif_limit_tm=%s, dif=%s, close_limit_tm=%s, close=%s, pre_dif_tm=%s, pre_dif=%s, pre_close_limit_tm=%s, pre_close=%s' % (
                        row.name, self.cross_type, dif.name, dif[DIF], close.name, close[CLOSE],
                        pre_dif.name, pre_dif[DIF], pre_close.name, pre_close[CLOSE]))
                continue

            # 解决DIF和DEA纠缠的问题：要求两个背离点对应的macd值不能太小。
            ldf = df[df.index <= dif.name]  # dif极值点之前的数据[包含dif极值点]
            if not self.is_tangle_by_dea_and_dif(macd, pre_macd, ldf[MACD]):
                log.debug(
                        '【%s, %s】纠缠, dif_limit_tm=%s, dif=%s, macd_limit_tm=%s, macd=%s, pre_dif_limit_tm=%s, pre_dif=%s, pre_macd_limit_tm=%s, pre_macd=%s' % (
                        row.name, self.cross_type, dif.name, dif[DIF], macd.name, macd[MACD],
                        pre_dif.name, pre_dif[DIF], pre_macd.name, pre_macd[MACD]))
                continue

            # 对背离点高度的要求：
            if not self.is_valid_by_dif_max(ldf[DIF], dif[DIF], pre_dif[DIF]):
                log.debug('【%s, %s】背离点高度检测, dif_limit_tm=%s, dif=%s, pre_dif_limit_tm=%s, pre_dif=%s' % (
                    row.name, self.cross_type, dif.name, dif[DIF], pre_dif.name, pre_dif[DIF]))
                continue

            # DIF和价格的差，至少有一个比较显著才能算显著背离。
            # 判断方法：(DIF极值涨跌幅的绝对值+价格极值涨跌幅的绝对值) > self.significance
            significance = self.calc_significance_of_divergence(dif, close, pre_dif, pre_close)
            if self.significance is not None and significance <= self.significance:
                log.debug(
                        '【%s, %s】显著背离检测, dif_limit_tm=%s, pre_dif_limit_tm=%s, significance=%s' % (
                        row.name, self.cross_type, dif.name, pre_dif.name, significance))
                continue

            divergences.append(
                Divergence(self.divergence_type, pre_dif_limit_tm=pre_cross[DIF_LIMIT_TM],
                           last_dif_limit_tm=row[DIF_LIMIT_TM],
                           significance=significance))
        return divergences

    def is_valid_by_dif_max(self, dif_series, dif, pre_dif):
        """
        判断是不是最大值
        采用过去250个bar内极值的最大值的绝对值作为参考，
        背离点中必须至少有一个极值的绝对值大于阈值dif_max[绝对值的最大值*dif_limit_factor]。
        :param dif_series:
        :param dif:
        :param pre_dif:
        :return:
        """
        dif_max = self.get_abs_max(dif_series, self.dif_limit_bar_num) * self.dif_limit_factor
        if not np.isnan(dif_max) and (abs(dif) < dif_max and abs(pre_dif) < dif_max):
            return False
        return True

    @staticmethod
    def get_limit_by_cross(df, row):
        dif_limit_tm, close_limit_tm, macd_limit_tm = row[DIF_LIMIT_TM], row[CLOSE_LIMIT_TM], row[MACD_LIMIT_TM]
        try:
            dif = df.loc[dif_limit_tm]  # 交叉点对应的DIF极值
            close = df.loc[close_limit_tm]  # 交叉点对应的价格极值
            macd = df.loc[macd_limit_tm]  # 交叉点对应的macd极值
            return dif, close, macd
        except:
            log.debug('【%s】获取极值为空' % row.name)
            return None, None, None

    @staticmethod
    def get_abs_max(series, num):
        """
        获取近num个bar内，绝对值的最大值
        :param series: Series类型
        :param num: 数量。最近多少个bar内计算最大值
        :return:
        """
        ser2 = series.iloc[-num:]
        max_val = np.nanmax(ser2)
        min_val = np.nanmin(ser2)
        return np.nanmax([abs(max_val), abs(min_val)])

    def is_tangle_by_dea_and_dif(self, macd, pre_macd, macd_ser):
        """
        判断dif和dea是否纠缠, 解决DIF和DEA纠缠在一起的问题：要求两个背离点对应的macd值不能太小。
        必须同时满足以下条件：
            1)abs(macd/pre_macd)>0.3
            2)max([abs(macd), abs(pre_macd)])/macd_max > 0.5

        :param macd: 当前bar的MACD值
        :param pre_macd: 前一个bar的MACD值
        :param macd_ser: Series类型，MACD的时间序列数据
        :return: 是-纠缠， 否-不纠缠
        """
        if abs(macd[MACD] / pre_macd[MACD]) <= 0.3:
            log.debug('【%s, %s】MACD、MACD_PRE纠缠, %s, %s' % (macd.name, pre_macd.name, macd[MACD], pre_macd[MACD]))
            return False
        macd_max = abs(self.get_abs_max(macd_ser, 250)) * 0.5
        if max([abs(macd[MACD]), abs(pre_macd[MACD])]) <= macd_max:
            log.debug('【%s, %s】与最大值相比，发生纠缠, %s, %s, %s' % (macd.name, pre_macd.name, macd[MACD], pre_macd[MACD], macd_max))
            return False
        return True

    @staticmethod
    def calc_significance_of_divergence(dif, close, pre_dif, pre_close):
        """
        检测是不是明显的背离： DIF和价格的差，至少有一个比较显著才能算显著背离。
        判断方法：DIF涨跌幅的绝对值+价格涨跌幅的绝对值>阈值
        :param dif: 当前bar的数据，Series类型，至少包含DIF
        :param close: 当前bar的数据，Series类型，至少包含CLOSE
        :param pre_dif: 前一个bar的数据，Series类型，至少包含DIF
        :param pre_close:前一个bar的数据，Series类型，至少包含CLOSE
        :return: True-是显著背离 False-不是显著背离
        """
        return abs((dif[DIF] - pre_dif[DIF]) / pre_dif[DIF]) + abs(close[CLOSE] - pre_close[CLOSE]) / pre_close[CLOSE]

    def _larger_than(self, val1, val2):
        """
        判断val1是否大于val2。由子类实现
        """
        raise NotImplementedError()

class TopDivergenceDetect(DivergenceDetect):
    """
        检测顶背离：价格创新高， DIF没有创新高
    """
    def __init__(self):
        super(TopDivergenceDetect, self).__init__()
        self.divergence_type = DivergenceType.Top
        self.cross_type = DEATH  # 根据死叉往前找顶背离

    def is_valid_by_zero_axis(self, dif, pre_dif):
        """
        判断两个dif极值点是否都在0轴以上
        :param dif:当前bar的dif值
        :param pre_dif:前一个bar的dif值
        :return:
        """
        return dif > 0 and pre_dif > 0

    def is_valid_by_close_and_dif(self, close, pre_close, dif, pre_dif):
        """
        判断是否满足价格创新高， DIF没有创新高。
        :param close:
        :param pre_close:
        :param dif:
        :param pre_dif:
        :return:
        """
        return pre_close < close and pre_dif >= dif

    def _larger_than(self, val1, val2):
        """
        判断val1是不是高于val2
        :param val1:
        :param val2:
        :return:
        """
        return val1 > val2

class BottomDivergenceDetect(DivergenceDetect):
    """
        检测底背离：价格创新低， DIF没有创新低
    """
    def __init__(self):
        super(BottomDivergenceDetect, self).__init__()
        self.divergence_type = DivergenceType.Bottom
        self.cross_type = GOLD  # 根据金叉往前找底背离

    def is_valid_by_zero_axis(self, dif, pre_dif):
        """
        判断两个dif极值点是否都在0轴以下
        :param dif:当前bar的dif值
        :param pre_dif:前一个bar的dif值
        :return:
        """
        return dif < 0 and pre_dif < 0

    def is_valid_by_close_and_dif(self, close, pre_close, dif, pre_dif):
        """
        判断是否满足价格创新低， DIF没有创新低。
        :param close:
        :param pre_close:
        :param dif:
        :param pre_dif:
        :return:
        """
        return pre_close > close and pre_dif <= dif

    def _larger_than(self, val1, val2):
        """判断val1是不是低于val2"""
        return val1 < val2

class Indicator:
    """
    检测MACD指标
    """
    def __init__(self):
        self.min_limit_detect = MinLimitDetect
        self.max_limit_detect = MaxLimitDetect
        self.top_detect = TopDivergenceDetect()
        self.bottom_detect = BottomDivergenceDetect()
        self.cross_detect = CrossDetect()

    def last_cross(self, df, idx):
        """
        检测索引为idx的bar,检测是否触发金叉或死叉，并且设置这根bar的金叉、死叉的值
        :param df: DataFrame类型。时间序列为索引
        :param idx: 位置。df的每一行的位置编号
        :return:
        """
        gold, death = False, False
        tm = df.iloc[idx].name
        cross_df = df[df.index <= tm]
        gold = self.cross_detect.is_cross(cross_df, GoldCross)
        if not gold:
            death = self.cross_detect.is_cross(cross_df, DeathCross)
        df.loc[tm, [GOLD, DEATH]] = gold, death

    def last_limit_point_tm(self, df, idx):
        """
        检测索引为idx的bar, 如果这个bar发生了金叉或死叉，根据交叉点查找3种极值[MACD，CLOSE, DIF]，并在当前bar，记录极值产生的时间
        :param df:
        :param idx:
        :return:
        """
        row = df.iloc[idx]
        tm = row.name
        if row[GOLD]:
            # 如果当前bar触发金叉，往前检测MinLimit
            dif_limit_tm, close_limit_tm, macd_limit_tm = self.get_limit_before_cross(df, tm, DEATH,
        elif row[DEATH]:
            # 如果当前bar触发死叉， 往前检测MaxLimit
            dif_limit_tm, close_limit_tm, macd_limit_tm = self.get_limit_before_cross(df, tm, GOLD,
        else:
            dif_limit_tm, close_limit_tm, macd_limit_tm = None, None, None

        # 在当前bar对应的数据结构中，记录以下3个极值点的时间：dif极值点、价格极值点、macd极值点
        df.loc[tm, [DIF_LIMIT_TM, CLOSE_LIMIT_TM, MACD_LIMIT_TM]] = dif_limit_tm, close_limit_tm, macd_limit_tm

    def get_last_divergences(self, df):
        """
        检测最近一个bar，是否发生顶背离或底背离
        :param df:
        :return:
        """
        divergences = self.bottom_detect.get_divergences(df)
        divergences += self.top_detect.get_divergences(df)
        return divergences

    def get_limit_before_cross(self, df, current_bar_tm, pre_cross_type, limit_detect):
        """
        获取三种极值的时间[MACD，CLOSE, DIF]
        :param df: DataFrame类型
        :param current_bar_tm: 当前bar的时间
        :param pre_cross_type: 前一个交叉点的类型
        :param limit_detect: 检测极值的类
        :return:
        """
        pre_cross_tm = self.get_pre_cross_tm(df, current_bar_tm, pre_cross_type)
        if not pre_cross_tm:
            limit_df = df[(df.index < current_bar_tm)]  # 检测极值的区间不能包含当前穿越点，可以包含前一个穿越点
        else:
            limit_df = df[(df.index < current_bar_tm) & (df.index >= pre_cross_tm)]
        if limit_df.empty:
            return None, None, None
        log.debug('[%s,%s]穿越点' % (current_bar_tm, pre_cross_tm))
        dif_limit_tm = limit_detect.get_dif_limit_tm_in(limit_df)
        close_limit_tm = limit_detect.get_close_limit_tm_in(limit_df)
        macd_limit_tm = limit_detect.get_macd_limit_tm_in(limit_df)
        return dif_limit_tm, close_limit_tm, macd_limit_tm

    @staticmethod
    def get_pre_cross_tm(df, cross_tm, pre_cross_type):
        """
        获取dif和dea前一个交叉点的时间
        :param df: DataFrame类型
        :param cross_tm: 交叉的时间
        :param pre_cross_type: 前一次交叉的类型[金叉或死叉]
        :return:
        """
        cross_df = df[(df.index < cross_tm) & (df[pre_cross_type])]
        if cross_df.empty:
            return None
        else:
            return cross_df.index[-1]

    @staticmethod
    def macd(df):
        """
        计算MACD的三个指标：DIFF, DEA, MACD
        DIFF=今日EMA（12）- 今日EMA（26）
        MACD= (DIFF－DEA)*2
        :param df:
        :return: 补充dif,dea,macd计算结果
        """
        close = df[CLOSE]
        if pd.__version__ >= "0.18.0":
            dif = close.ewm(span=SHORT).mean() - close.ewm(span=LONG).mean()
            dea = dif.ewm(span=MID).mean()
        else:
            # 聚宽使用的pandas库版本比较低
            dif = pd.ewma(close, span=SHORT) - pd.ewma(close, span=LONG)
            dea = pd.ewma(dif, span=MID)
        macd = (dif - dea) * 2
        df[DIF], df[DEA], df[MACD] = dif, dea, macd

class MacdCache:
    """
    macd缓存：缓存历史数据，用于检测金叉、死叉、背离。缓存的数据包含以下几项：
        - bars:dict类型, key-股票代码, value-DataFrame类型,包含的列:
            - close[收盘价]/dif/dea/macd
            - gold[是否金叉]/death[是否死叉]
            - dif_limit_tm[dif极值的时间]
            - close_limit_tm[收盘价极值的时间]
            - macd_limit_tm[macd极值的时间]
            注：触发金叉或死叉后，以当前交叉点往前检测寻找价格、DIF、MACD极值点。并记录极值的时间
        - divergences: dict类型， key-股票代码， value - 触发的背离，包含顶背离和底背离, 只缓存最新一根bar触发的背离
    """
    def __init__(self, count, stocks = None):
        self.indicator = Indicator()  # macd指标检测接口
        self.bar_cache_num = count    # 数量. 缓存多少个bar的检测结果
        if not stocks: stocks = list()
        self.bars = dict()
        self.stocks = stocks
        self.divergences = dict()         # 当前收盘后触发的背离
        self.bar_cache_cols = COLS
        self.__init_cache()

    def __init_cache(self):
        """
        如果股票池不为空，初始化后就开始缓存股票池的数据
        :return:
        """
        for code in self.stocks:
            try:
                self.__init_single_cache(code)
                log.debug('【macd缓存初始化end】code={}.'.format(code))
            except:
                log.debug('【macd缓存初始化】异常：code={}, exception={}'.format(code, traceback.format_exc()))

    def __init_single_cache(self, code):
        """
        初始化单支股票的缓存
        :param code:
        :return:
        """
        df = get_df()
        #df = self.dbkline.get_bars(code, count=self.bar_cache_num + EXTRA_LOAD_BAR_NUM, unit=self.period, fields=[CLOSE], end_tm=init_tm)
        if df.empty:
            log.debug('【macd缓存初始化】警告：code={},  bars empty'.format(code))
            return
        self.supply_cols(df, self.bar_cache_cols)
        self.indicator.macd(df)
        for idx in range(0, len(df)):
            self.update_last_bar_single_stock(df, code, idx, self.indicator)

    def update_last_bar_single_stock(self, df, code, idx, indicator):
        """
        更新单支股票的缓存数据
        :param df: 缓存的bar
        :param code: str类型。股票代码
        :param idx: int类型。bar的位置。当前需要更新的bar,在df中的位置。
        :param indicator: Indicator类型。 用于指标计算以信号检测的实例。
        :return:
        """
        indicator.last_cross(df, idx)  # 记录idx位置的bar是否发生金叉死叉
        indicator.last_limit_point_tm(df, idx)  # 记录极值点
        self.bars[code] = df

        row = df.iloc[idx]
        tm = row.name
        if df.iloc[-1].name == tm:  # 初始化缓存数据的时候：历史数据不检测背离
            self.update_divergences(df.iloc[:idx + 1], code)

        divergences = self.divergences[code] if code in self.divergences.keys() else []
        log.debug('【%s, %s】MACD更新完成, row=%s, divergences=%s' % (code, tm, row.to_dict(), Divergence.to_json_list(divergences)))

    def update_cache(self, last_tm=None):
        """
        指定当前时间，更新股票池中所有股票的缓存。
        :param last_tm:当前时间
        :return:
        """
        for code in self.stocks:
            log.debug('【%s, %s】MACD指定时间更新缓存' % (code, last_tm))
            df = get_df()
            #df = self.dbkline.get_bars(code, count=1, end_tm=last_tm, unit=self.period, fields=['close'])
            if df.empty:
                log.warn('【%s, %s】查询k线数据为空' % (code, last_tm))
                continue
            last = df.iloc[-1]
            if last.empty:
                continue
            close = last[CLOSE]
            if np.isnan(close):
                continue
            self.update_single_cache(code, df.iloc[-1].name, close)  # NOTICE: 取查询的结果与已经缓存的结果对比。

    def update_single_cache(self, code, last_tm, last_close):
        """
        指定当前时间，更新一只股票的缓存。
        :param code: 股票代码
        :param last_tm: 指定的时间
        :param last_close: 指定时间的收盘价
        :return:
        """
        log.debug('【%s, %s】MACD查询数据close=%s，缓存更新' % (code, last_tm, last_close))
        if code not in self.bars.keys():
            self.__init_single_cache(code, last_tm)
            return

        # 发生除权除息后，重新计算缓存的数据
        factors = self.dbkline.get_bars(code, count=2, end_tm=last_tm, unit='daily', fields=[ADJ_FACTOR])
        if factors.empty:
            log.debug('【%s, %s】复权因子为空', code, last_tm)
            return

        factors = factors[~(np.isnan(factors[ADJ_FACTOR]))]
        # 复权因子发生变化，重新初始化缓存数据
        if len(factors) > 1 and factors.iloc[0]['factor'] != factors.iloc[1]['factor']:
            self.__init_single_cache(code, last_tm)
            log.debug('【%s, %s】复权因子发生变化,重新初始化缓存', code, last_tm)
            return
        df = self.bars[code]
        if df.empty:
            return
        if last_tm <= df.iloc[-1].name:  # 已经是最新的数据，不需要更新
            return

        df.at[last_tm, CLOSE] = last_close  # 追加最新的一根bar
        if len(df) > DEFAULT_LOAD_BAR_NUM + EXTRA_LOAD_BAR_NUM:
            df = df.iloc[1:].copy()  # 超过缓存数量，去掉最早的一根bar

        idx = len(df) - 1  # 最后一根bar的位置
        self.indicator.macd(df)  # 计算dif,dea,macd
        self.update_last_bar_single_stock(df, code, idx, self.indicator)

    @staticmethod
    def supply_cols(df, add_cols):
        """
        设置df的列
        :param df:Dataframe类型。缓存的数据。
        :param add_cols:要设置的列
        :return:
        """
        df_cols = df.columns.values.tolist()
        for col in add_cols:
            if col in df_cols: continue
            df[col] = None

    def update_divergences(self, df, code):
        """
        更新缓存中的背离信息。只缓存最近一根bar产生的背离
        :param df:Dataframe类型。缓存的数据。
        :param code:股票代码
        :return:
        """
        # 检测背离
        if code not in self.divergences.keys(): self.divergences[code] = []
        # 记录当前点产生的所有的背离[同一个极值点的末端，可能检测到多个起始点不同的背离]
        self.divergences[code] = self.indicator.get_last_divergences(df)
