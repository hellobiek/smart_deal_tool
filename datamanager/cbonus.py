# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import pandas as pd
from base.cdate import str_to_datetime, int_to_datetime, years_ago, datetime_to_int
class CBonus(object):
    def __init__(self, fpath = '/data/tdx/base/bonus.csv'):
        self.fpath = fpath
        self.data = self.init()

    def init(self):
        df = pd.read_csv(self.fpath, header=0, encoding="utf8")
        df['code'] = df['code'].map(lambda x: str(x).zfill(6))
        return df

    def get_bonus(self, code=None):
        """ 获取某只股票的高送转信息"""
        if code is not None:
            return self.data[self.data["code"] == code]
        return self.data

    def get_dividend_rate(self, mdate, code, price, nyear = 3):
        """
        求某只股票在指定时间点的nyear年平均股息率
        :param mdate:
        :param code:
        :param price:
        :param nyear:
        :return:
        """
        end = int_to_datetime(mdate)
        start = years_ago(years = nyear, from_date=end)
        bonus_df = self.get_bonus(code).sort_values(['date'], ascending=True)
        filter_df = bonus_df[(bonus_df["type"] == 1) & 
                             (bonus_df["date"] >= datetime_to_int(start)) & 
                             (bonus_df["date"] <= datetime_to_int(end))]
        total_money = 0.0
        for idx, item in filter_df.iterrows(): total_money += item["money"] / 10
        return total_money / (nyear * price)

if __name__ == '__main__':
    cbonus = CBonus()
