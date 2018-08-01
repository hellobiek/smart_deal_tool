#coding=utf-8
import const as ct
import pandas as pd
import tushare as ts
from cnmysql import CNMySQL

if __name__ == "__main__":
    cmy = CNMySQL(ct.DB_NEW_INFO)
    all_tables = cmy.get_all_tables()
