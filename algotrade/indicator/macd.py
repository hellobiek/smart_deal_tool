import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import numpy as np
import pandas as pd
from enum import Enum
from base.clog import getLogger
from pyalgotrade import dataseries
from pyalgotrade.utils import collections
log = getLogger(__name__)
GOLD = 'gold'  # 金叉
DEATH = 'death'  # 死叉
NOSIGNAL = 'nosignal' #无信号
# 极值的调节因子。用于匹配多个近似的极值点。
# 多个近似的极值点，取离当前交叉点最近的一个。
LIMIT_DETECT_LIMIT_FACTOR = 0.99
# 背离检测：最多使用DIVERGENCE_DETECT_MOST_LIMIT_NUM个相邻的极值点，两两组合检测背离
DIVERGENCE_DETECT_MOST_LIMIT_NUM = 5
# 背离检测：DIF涨跌幅的绝对值+价格涨跌幅的绝对值。用于判断是不是一个比较显著的背离。
DIVERGENCE_DETECT_SIGNIFICANCE = 0.35
# 背离检测：对背离点高度的要求。采用过去250个bar内极值的最大值作为参考，背离点中必须至少有一个极值小于最大值的【20%】。
DIVERGENCE_DETECT_DIF_LIMIT_BAR_NUM = 250
DIVERGENCE_DETECT_DIF_LIMIT_FACTOR = 0.5
class EventWindow(object):
    """An EventWindow class is responsible for making calculation over a moving window of values.

    :param windowSize: The size of the window. Must be greater than 0.
    :type windowSize: int.
    :param dtype: The desired data-type for the array.
    :type dtype: data-type.
    :param skipNone: True if None values should not be included in the window.
    :type skipNone: boolean.

    .. note::
        This is a base class and should not be used directly.
    """
    def __init__(self, windowSize, dtype=float, skipNone = False):
        assert(windowSize > 0)
        assert(isinstance(windowSize, int))
        self.__multiplier = (2.0 / (windowSize + 1))
        self.__values  = collections.NumPyDeque(windowSize, dtype)
        self.__weights = collections.NumPyDeque(windowSize, dtype)
        self.__windowSize = windowSize
        self.__skipNone = skipNone
        self.initWeights()
        self.__weightsSum = self.__weights.data().sum()

    def initWeights(self):
        for i in range(self.__windowSize):
            self.__weights.append((1 - self.__multiplier) ** (self.__windowSize - i - 1))

    def getWeightedValue(self):
        return np.dot(self.__values.data(), self.__weights.data()) / self.__weightsSum

    def onNewValue(self, dateTime, value):
        if value is not None or not self.__skipNone:
            self.__values.append(value)

    def getValues(self):
        """Returns a numpy.array with the values in the window."""
        return self.__values.data()

    def getWindowSize(self):
        """Returns the window size."""
        return self.__windowSize

    def windowFull(self):
        return len(self.__values) == self.__windowSize

    def getValue(self):
        """Override to calculate a value using the values in the window."""
        raise NotImplementedError()

class EMAEventWindow(EventWindow):
    def __init__(self, period, skipNone = False):
        assert(period > 1)
        super(EMAEventWindow, self).__init__(period, skipNone = skipNone)
        self.__value = None

    def onNewValue(self, dateTime, value):
        super(EMAEventWindow, self).onNewValue(dateTime, value)
        if value is not None and self.windowFull():
            self.__value = self.getWeightedValue() 

    def getValue(self):
        return self.__value

class EventBasedFilter(dataseries.SequenceDataSeries):
    """An EventBasedFilter class is responsible for capturing new values in a :class:`pyalgotrade.dataseries.DataSeries`
    and using an :class:`EventWindow` to calculate new values.

    :param dataSeries: The DataSeries instance being filtered.
    :type dataSeries: :class:`pyalgotrade.dataseries.DataSeries`.
    :param eventWindow: The EventWindow instance to use to calculate new values.
    :type eventWindow: :class:`EventWindow`.
    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """
    def __init__(self, dataSeries, eventWindow, maxLen=None):
        super(EventBasedFilter, self).__init__(maxLen)
        self.__dataSeries = dataSeries
        self.__eventWindow = eventWindow
        self.__dataSeries.getNewValueEvent().subscribe(self.__onNewValue)

    def __onNewValue(self, dataSeries, dateTime, value):
        # Let the event window perform calculations.
        self.__eventWindow.onNewValue(dateTime, value)
        # Get the resulting value
        newValue = self.__eventWindow.getValue()
        # Add the new value.
        self.appendWithDateTime(dateTime, newValue)

    def getDataSeries(self):
        return self.__dataSeries

    def getEventWindow(self):
        return self.__eventWindow

class CrossDetect:
    """
    检测金叉死叉
    """
    @staticmethod
    def is_cross(pre_val, now_val, cross):
        """
        检测最后一根bar是不是定义的交叉类型
        :param df: DataFrame类型。缓存的数据，最后一条记录是待检测的bar
        :param cross: 金叉或死叉
        :return:
        """
        if pre_val is None or now_val is None: return False
        if not cross.is_cross(pre_val, now_val):
            log.debug('%s 没有穿过, pre_val=%s, now_val=%s' % (cross, pre_val, now_val))
            return False
        log.debug('%s 穿过, pre_val=%s, now_val=%s' % (cross, pre_val, now_val))
        return True

class GoldCross:
    """
    定义金叉
    """
    def __init__(self, cdate, dif, dif_date, macd, macd_date, close, close_date):
        self.type       = GOLD
        self.cdate      = cdate 
        self.dif        = dif 
        self.dif_date   = dif_date
        self.macd       = macd
        self.macd_date  = macd_date
        self.close      = close
        self.close_date = close_date

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
    def __init__(self, cdate, dif, dif_date, macd, macd_date, close, close_date):
        self.type       = DEATH
        self.cdate      = cdate
        self.dif        = dif 
        self.dif_date   = dif_date
        self.macd       = macd
        self.macd_date  = macd_date
        self.close      = close
        self.close_date = close_date

    @staticmethod
    def is_cross(pre_macd, macd):
        """
        判断是否死叉
        :param pre_macd: 前一个bar的macd
        :param macd: 当前bar的macd
        :return:
        """
        return pre_macd >= 0 > macd

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
        pass

    @staticmethod
    def to_json_list(obj_list):
        """
        将包含实例的数组转换成json字符串数组
        :param obj_list:
        :return:
        """
        dl = list()
        for obj in obj_list:
            if obj:
                dl.append(obj.to_json())
            else:
                dl.append(None)
        return dl

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
    def __init__(self, dtype, dif, close, cross_date, pre_dif, pre_close, pre_cross_date, significance):
        self.type = dtype
        self.dif = dif
        self.close = close
        self.cross_date = cross_date
        self.pre_dif = pre_dif
        self.pre_close = pre_close
        self.pre_cross_date = pre_cross_date
        self.significance = significance  # 背离的可见度

    def to_json(self):
        return {
            'type': self.type,
            'dif':  self.dif,
            'close': self.close,
            'cross_date': str(self.cross_date),
            'pre_dif': self.pre_dif,
            'pre_close': self.pre_close,
            'pre_cross_date': str(self.pre_cross_date),
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
        pass

    def is_valid_by_close_and_dif(self, close, pre_close, dif, pre_dif):
        """
        验证两个极值点的dif和close，是否满足背离要求。具体的验证方法由子类实现
        :param close:
        :param pre_close:
        :param dif:
        :param pre_dif:
        :return:
        """
        pass

    def get_divergences(self, crosses, dif_series, macd_series):
        """
        检测最近一个bar是否发生背离
        :param: cross: 所有金叉和死叉的信号
        :return: divergences list
        """
        divergences = list()
        if len(crosses) < 2: return divergences

        current_index = len(crosses) - 1
        if crosses[current_index].type != self.cross_type: return divergences

        pre_cross_lists = list()
        for index in range(current_index - 1, -1, -1):
            if crosses[index].type == self.cross_type: pre_cross_lists.append(index)
            if len(pre_cross_lists) >= self.most_limit_num: break
        log.debug("pre_cross_lists:%s" % pre_cross_lists)

        if len(pre_cross_lists) < 1:
            log.debug("只有一个%s极值点" % self.cross_type)
            return divergences

        now_cross = crosses[current_index]
        current_date, dif, macd, close = now_cross.cdate, now_cross.dif, now_cross.macd, now_cross.close
        if dif is None or close is None or macd is None:
            log.debug("类型:%s, 日期:%s, dif:%s, dif date:%s, macd:%s, macd date:%s, close:%s, close date:%s diff 没有意义" %\
                (now_cross.type, now_cross.cdate, now_cross.dif, now_cross.dif_date, now_cross.macd, now_cross.macd_date, now_cross.close, now_cross.close_date))
            return divergences

        for index in range(len(pre_cross_lists) - 1, -1, -1):
            pre_cross = crosses[pre_cross_lists[index]]
            pre_date, pre_dif, pre_close, pre_macd = pre_cross.cdate, pre_cross.dif, pre_cross.close, pre_cross.macd
            log.debug("begin computing pre date:%s, current date:%s, " % (pre_date, current_date))
            if pre_dif is None or pre_close is None or pre_macd is None:
                log.debug("类型:%s, 日期:%s, dif:%s, dif date:%s, macd:%s, macd date:%s, close:%s, close date:%s 前极值点没有意义" %\
                (pre_cross.type, pre_cross.cdate, pre_cross.dif, pre_cross.dif_date, pre_cross.macd, pre_cross.macd_date, pre_cross.close, pre_cross.close_date))
                continue

            # 分别对比两个点的价格以及dif的高低关系.顶背离：价格创新高，dif没有创新高，底背离：价格创新低，dif没有创新低
            if not self.is_valid_by_close_and_dif(close, pre_close, dif, pre_dif):
                log.debug("极值点价格和dif分别比较, type=%s, pre_date:%s, pre_dif=%s, pre_close=%s, date:%s, dif=%s, close=%s" % \
                                                    (self.cross_type, pre_date, pre_dif, pre_close, current_date, dif, close))
                continue

            # 解决DIF和DEA纠缠的问题：要求两个背离点对应的macd值不能太小。
            if not self.is_tangle_by_dea_and_dif(macd, pre_macd, macd_series):
                log.debug('dif和dea发生纠缠, type:%s, pre_date:%s, pre_dif=%s, pre_macd=%s, date:%s, dif=%s, macd=%s' % \
                                            (self.cross_type, pre_date, pre_dif, pre_macd, current_date, dif, macd))
                continue

            # 对背离点高度的要求：
            if not self.is_valid_by_dif_max(dif, pre_dif, dif_series):
                log.debug('背离点高度检测, type:%s, pre_date:%s, pre_dif=%s, date:%s, dif=%s' % \
                                        (self.cross_type, pre_date, pre_dif, current_date, dif))
                continue

            # dif和价格的差，至少有一个比较显著才能算显著背离。
            # 判断方法：(dif极值涨跌幅的绝对值+价格极值涨跌幅的绝对值) > self.significance
            significance = self.calc_significance_of_divergence(dif, close, pre_dif, pre_close)
            if self.significance is not None and significance <= self.significance:
                log.debug('显著背离检测, type:%s, pre_date:%s, pre_dif=%s, date:%s, dif=%s, significance=%s' % \
                                    (self.cross_type, pre_date, pre_dif, current_date, dif, significance))
                continue

            log.debug('找到新背离，type:%s, dif:%s. close:%s, current_date:%sm pre_dif:%s, pre_close:%s, pre_date:%s, significance:%s'%\
                                        (self.divergence_type, dif, close, current_date, pre_dif, pre_close, pre_date, significance))
            divergences.append(Divergence(self.divergence_type, dif, close, current_date, pre_dif, pre_close, pre_date, significance))
        return divergences

    def is_valid_by_dif_max(self, dif, pre_dif, dif_series):
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
        if not dif_max and (abs(dif) < dif_max and abs(pre_dif) < dif_max): return False
        return True

    @staticmethod
    def get_abs_max(series, num):
        """
        获取近num个bar内，绝对值的最大值
        :param series: Series类型
        :param num: 数量。最近多少个bar内计算最大值
        :return:
        """
        end_index = len(series) - 1
        start_index = max(end_index - num - 1, -1)
        max_val = min_val = series[end_index]
        for index in range(end_index, start_index, -1):
            if series[index] is not None:
                if series[index] > max_val: max_val = series[index] 
                if series[index] < min_val: min_val = series[index] 
        return max(abs(max_val), abs(min_val))

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
        if abs(macd / pre_macd) <= 0.3:
            log.debug('MACD:%s 与 MACD_PRE:%s纠缠' % (macd, pre_macd))
            return False

        macd_max = abs(self.get_abs_max(macd_ser, self.dif_limit_bar_num)) * 0.5
        if max(abs(macd), abs(pre_macd)) <= macd_max:
            log.debug('最大值:%s, pre_macd:%s , macd:%s 发生纠缠' % (macd_max, macd, pre_macd))
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
        return abs((dif - pre_dif) / pre_dif) + abs(close - pre_close) / pre_close

    def _larger_than(self, val1, val2):
        """
        判断val1是否大于val2。由子类实现
        """
        pass

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
        return pre_close < close and pre_dif > dif

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
        return pre_close > close and pre_dif < dif

    def _larger_than(self, val1, val2):
        """判断val1是不是低于val2"""
        return val1 < val2

class Detect:
    @staticmethod
    def get_date_by_index(series, index):
        datetimes = series.getDateTimes()
        return datetimes[index] 

    @staticmethod
    def get_index_by_date(series, cdate):
        cdates = series.getDateTimes()
        for index in range(len(cdates) -1, -1, -1):
            if cdates[index] == cdate: return index
        return -1

class MaxLimitDetect(Detect):
    """
     检测极值：最大值的时间。用于检测3种极值的时间，3种极值分别是：DIF/CLOSE/MACD
    """
    @staticmethod
    def __get_max_val_in(series, start_index, end_index):
        max_val = series[end_index]
        for index in range(start_index, end_index):
            value = series[index]
            if value is not None and value > max_val: max_val = value
        return max_val

    @staticmethod
    def __get_max_limit_info_in(series, start_index, end_index, max_val):
        # 所有候选的极值点，使用LIMIT_DETECT_LIMIT_FACTOR是模糊匹配
        if max_val <= 0: return None, None
        limit_index = -1
        for index in range(end_index, start_index - 1, -1):
            value = series[index]
            if value is not None and value >= max_val * LIMIT_DETECT_LIMIT_FACTOR: limit_index = index
        value = series[limit_index]
        vdate = MaxLimitDetect.get_date_by_index(series, limit_index) 
        return value, vdate

    @classmethod
    def get_close_limit_info_in(cls, series, start_date, end_date):
        if start_date == end_date: raise Exception("MaxLimitDetect close unexpected start_date:%s and end_date:%s" % (start_date, end_date))
        start_index = cls.get_index_by_date(series, start_date)
        end_index   = cls.get_index_by_date(series, end_date)
        if end_index == -1: Exception("unexpected end_date:%s" % end_date)
        if start_index == -1: return None, None
        max_val = cls.__get_max_val_in(series, start_index, end_index)
        return cls.__get_max_limit_info_in(series, start_index, end_index, max_val)

    @classmethod
    def get_limit_info_in(cls, series, start_date, end_date):
        if start_date == end_date: raise Exception("MaxLimitDetect limit unexpected start_date:%s and end_date:%s" % (start_date, end_date))
        start_index = cls.get_index_by_date(series, start_date)
        end_index   = cls.get_index_by_date(series, end_date)
        if end_index == -1: Exception("unexpected end_date:%s" % end_date)
        if start_index == -1: return None, None
        max_val = cls.__get_max_val_in(series, start_index, end_index)
        return cls.__get_max_limit_info_in(series, start_index, end_index, max_val)

class MinLimitDetect(Detect):
    """
    检测极值：最小值的时间。用于检测3种极值的时间，3种极值分别是：DIF/CLOSE/MACD
    """
    @staticmethod
    def __get_min_val_in(series, start_index, end_index):
        min_val = series[end_index]
        for index in range(start_index, end_index):
            value = series[index]
            if value is not None and value < min_val: min_val = value
        return min_val

    @staticmethod
    def __get_min_limit_info_in(series, start_index, end_index, min_val):
        # 所有候选的极值点，使用LIMIT_DETECT_LIMIT_FACTOR是模糊匹配
        # 要求当前区间内的dif(macd)最小值必须在零轴下。
        if min_val >= 0: return None, None
        limit_index = -1
        for index in range(end_index, start_index - 1, -1):
            value = series[index]
            if value is not None and value < min_val * LIMIT_DETECT_LIMIT_FACTOR: limit_index = index
        value = series[limit_index]
        vdate = MinLimitDetect.get_date_by_index(series, limit_index) 
        return value, vdate

    @classmethod
    def get_close_limit_info_in(cls, series, start_date, end_date):
        if start_date == end_date: raise Exception("MinLimitDetect close unexpected start_date:%s and end_date:%s" % (start_date, end_date))
        start_index = cls.get_index_by_date(series, start_date)
        end_index   = cls.get_index_by_date(series, end_date)
        if end_index == -1: Exception("unexpected end_date:%s" % end_date)
        if start_index == -1: return None, None
        min_val = cls.__get_min_val_in(series, start_index, end_index)
        if min_val <= 0: Exception("unexpected min_val:%s" % min_val)
        limit_index = -1
        for index in range(end_index, start_index - 1, -1):
            value = series[index]
            if value is not None and value < min_val / LIMIT_DETECT_LIMIT_FACTOR: limit_index = index
        value = series[limit_index]
        vdate = MinLimitDetect.get_date_by_index(series, limit_index) 
        return value, vdate

    @classmethod
    def get_limit_info_in(cls, series, start_date, end_date):
        if start_date == end_date: raise Exception("MinLimitDetect limit unexpected start_date:%s and end_date:%s" % (start_date, end_date))
        start_index = cls.get_index_by_date(series, start_date)
        end_index   = cls.get_index_by_date(series, end_date)
        if end_index == -1: Exception("unexpected end_date:%s" % end_date)
        if start_index == -1: return None, None
        min_val = cls.__get_min_val_in(series, start_index, end_index)
        return cls.__get_min_limit_info_in(series, start_index, end_index, min_val)

class Macd(dataseries.SequenceDataSeries):
    def __init__(self, feed, fastEMA, slowEMA, signalEMA, maxLen, instrument):
        """Moving Average Convergence-Divergence indicator as described in http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_convergence_divergence_macd.
        :param dataSeries: The DataSeries instance being filtered.
        :type dataSeries: :class:`pyalgotrade.dataseries.DataSeries`.
        :param fastEMA: The number of values to use to calculate the fast EMA.
        :type fastEMA: int.
        :param slowEMA: The number of values to use to calculate the slow EMA.
        :type slowEMA: int.
        :param signalEMA: The number of values to use to calculate the signal EMA.
        :type signalEMA: int.
        :param maxLen: The maximum number of values to hold.
            Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
            opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
        :type maxLen: int.
        """
        assert(fastEMA > 0)
        assert(slowEMA > 0)
        assert(fastEMA < slowEMA)
        assert(signalEMA > 0)
        super(Macd, self).__init__(maxLen)
        # We need to skip some values when calculating the fast EMA in order for both EMA
        # to calculate their first values at the same time.
        # I'M FORCING THIS BEHAVIOUR ONLY TO MAKE THIS FITLER MATCH TA-Lib MACD VALUES.
        self.__skipNum = max(fastEMA, slowEMA, signalEMA)
        self.__instrument = instrument
        self.__fastEMAWindow = EMAEventWindow(fastEMA)
        self.__slowEMAWindow = EMAEventWindow(slowEMA)
        self.__signalEMAWindow = EMAEventWindow(signalEMA)
        self.__signal = dataseries.SequenceDataSeries(maxLen) #dea
        self.__histogram = dataseries.SequenceDataSeries(maxLen) #macd
        self.__close_prices = feed[instrument].getPriceDataSeries()
        self.__cross = dataseries.SequenceDataSeries(maxLen) #dead cross signals and gold cross signals
        self.divergences = list()
        self.cross_detect = CrossDetect()
        self.max_limit_detect = MaxLimitDetect
        self.min_limit_detect = MinLimitDetect
        self.top_divergence_detect = TopDivergenceDetect()
        self.bottom_divergence_detect = BottomDivergenceDetect()
        self.__close_prices.getNewValueEvent().subscribe(self.__onNewValue)

    def newCross(self, cross_type, cross_date, limit_dif, limit_dif_date, macd, macd_date, close, close_date):
        if cross_type == GOLD:
            return GoldCross(cross_date, limit_dif, limit_dif_date, macd, macd_date, close, close_date) 
        else:
            return DeathCross(cross_date, limit_dif, limit_dif_date, macd, macd_date, close, close_date)

    def createNewCross(self, current_date, pre_cross_type, current_cross_type, detect):
        """
        获取三种极值的时间和极值[MACD，CLOSE, DIF]
        :param current_date: 当前bar的date
        :param pre_cross_type: 前一个交叉点的类型
        :param current_cross_type: 当前交叉点的类型
        :param detect: 极值检测类对象
        :return: new class
        """
        pre_cross_signal_date = self.getPreSignalDate(pre_cross_type)
        if pre_cross_signal_date is None: return self.newCross(current_cross_type, current_date, None, None, None, None, None, None)
        dif, dif_date = detect.get_limit_info_in(self, pre_cross_signal_date, current_date)
        close, close_date = detect.get_close_limit_info_in(self.__close_prices, pre_cross_signal_date, current_date)
        macd, macd_date = detect.get_limit_info_in(self.__histogram, pre_cross_signal_date, current_date)
        return self.newCross(current_cross_type, current_date, dif, dif_date, macd, macd_date, close, close_date)

    def getPreSignalDate(self, pre_cross_type):
        """
        获取dif和dea前一个交叉点的时间
        :param pre_cross_type: 前一次交叉的类型[金叉或死叉]
        :return:
        """
        slength = len(self.__cross)
        if slength == 0: return None
        for x in range(slength - 1, -1, -1):
            if self.__cross[x].type == pre_cross_type: return self.__cross[x].cdate
        return None

    def getDif(self):
        return self

    def getDea(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the EMA over the MACD."""
        return self.__signal

    def getMacd(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the histogram (the difference between the MACD and the Signal)."""
        return self.__histogram

    def updateDivergences(self):
        """
        更新缓存中的背离信息。只缓存最近一根bar产生的背离
        :param code:股票代码
        :return:
        """
        # 检测背离, 记录当前点产生的所有的背离[同一个极值点的末端，可能检测到多个起始点不同的背离]
        self.divergences  = self.top_divergence_detect.get_divergences(self.__cross, self, self.__histogram)
        self.divergences += self.bottom_divergence_detect.get_divergences(self.__cross, self, self.__histogram)

    def __onNewValue(self, dataSeries, dateTime, value):
        self.__slowEMAWindow.onNewValue(dateTime, value)
        self.__fastEMAWindow.onNewValue(dateTime, value)
        fastValue = self.__fastEMAWindow.getValue()
        slowValue = self.__slowEMAWindow.getValue()
        if fastValue is None or slowValue is None:
            diffValue = None
        else:
            diffValue = fastValue - slowValue

        # Make the first MACD value available as soon as the first signal value is available.
        # I'M FORCING THIS BEHAVIOUR ONLY TO MAKE THIS FITLER MATCH TA-Lib MACD VALUES.
        self.__signalEMAWindow.onNewValue(dateTime, diffValue)
        deaValue = self.__signalEMAWindow.getValue()
        if diffValue is None or deaValue is None:
            macdValue = None
        else:
            macdValue = 2 * (diffValue - deaValue)

        self.appendWithDateTime(dateTime, diffValue) #dif
        self.__signal.appendWithDateTime(dateTime, deaValue) #dea
        self.__histogram.appendWithDateTime(dateTime, macdValue) #macd

        #import datetime
        #if dateTime == datetime.datetime(2017, 11, 29, 0, 0, 0):
        #    import pdb
        #    pdb.set_trace()
        
        if len(self) >= self.__skipNum:
            preMacdValue = self.__histogram[-2]
            if self.cross_detect.is_cross(preMacdValue, macdValue, GoldCross):
                cross = self.createNewCross(dateTime, DEATH, GOLD, self.min_limit_detect)
                log.debug("code:%s 类型:%s, 日期:%s, dif:%s, dif date:%s, macd:%s, macd date:%s, close:%s, close date:%s" %\
                (self.__instrument, cross.type, cross.cdate, cross.dif, cross.dif_date, cross.macd, cross.macd_date, cross.close, cross.close_date))
                self.__cross.appendWithDateTime(dateTime, cross)
            elif self.cross_detect.is_cross(preMacdValue, macdValue, DeathCross):
                cross = self.createNewCross(dateTime, GOLD, DEATH, self.max_limit_detect)
                log.debug("code:%s 类型:%s, 日期:%s, dif:%s, dif date:%s, macd:%s, macd date:%s, close:%s, close date:%s" %\
                (self.__instrument, cross.type, cross.cdate, cross.dif, cross.dif_date, cross.macd, cross.macd_date, cross.close, cross.close_date))
                self.__cross.appendWithDateTime(dateTime, cross)
            else:
                log.debug("%s 没有信号" % dateTime)
        else:
            log.debug("%s 还需要skip" % dateTime)
        
        self.updateDivergences()
