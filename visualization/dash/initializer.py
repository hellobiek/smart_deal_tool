# -*- coding: utf-8 -*-
from cstock_info import CStockInfo
def update_code_list():
    base_df = CStockInfo().get()
    return base_df.code.tolist()
