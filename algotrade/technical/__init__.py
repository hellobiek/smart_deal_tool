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
    def is_cross(df, cross):
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
        if not cross.is_cross(pre_row[MACD], row[MACD]):
            log.debug(u'【%s】没有穿过, macd=%s, pre_macd=%s' % (row.name, row[MACD], pre_row[MACD]))
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
