#-*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
from simpledbf import Dbf5
FIEL_PATH = '/tongdaxin/T0002/hq_cache/base.dbf'
######################
#SC     ：市场
#GPDM   ：股票代码
#GXRQ   : 更新日期
#ZGB    ：总股份
#GJG    ：国家股
#FQRFRG ：发起人股
#FRG    ：法人股
#BG     ：B股
#HG     ：H股
#LTAG   ：流通A股
#ZGG    ：职工股
#ZPG    ：转配股
#ZZC    ：总资产
#LDZC   ：流动资产
#GDZC   ：固定资产
#WXZC   ：无形资产
#CQTZ   ：股东人数
#LDFZ   ：流动负债
#CQFZ   ：少数股权
#ZBGJJ  ：资本公积金
#JZC    : 净资产
#ZYSY   : 主营收益 
#ZYLY   : 营业成本
#QTLY   : 应收账款
#YYLY   : 营业利润
#TZSY   : 投资收益
#BTSY   : 经营现金流
#YYWSZ  : 总现金流
#SNSYTZ : 存货
#LYZE   : 利润总额
#SHLY   : 税后利润
#JLY    : 净利润
#WFPLY  : 未分配利润
#TZMGJZ : 调整后净资产
#DY     : 地域
#HY     : 行业
#ZBNB   : 资料月份 9 代表三季报 12代表年报
#SSDATE : 上市日期
#MODIDATE : Nan 
#GDRS   : Nan
######################
class StockBasicInfoReader(object):
    def __init__(self, file_path = FIEL_PATH):
        self.file_path = file_path

    def read(self):
        dbf = Dbf5(self.file_path)
        df = dbf.to_dataframe()
        return df

if __name__ == '__main__':
    sbir = StockBasicInfoReader()
    df = sbir.read()
