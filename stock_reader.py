# coding=utf-8
import os
import const as ct
import pandas as pd
from log import getLogger
from cstock import CStock
logger = getLogger(__name__)

succeed_list = list()
def read_stock_csv(stock_dir):
    for filename in os.listdir(stock_dir):
        #logger.info("start :%s" % filename)
        code = filename.split('.')[0]
        d_table_name = "%s_D" % code
        stock = CStock(code, ct.DB_INFO)
        #logger.info("created :%s" % filename)
        try:
            df = pd.read_csv(os.path.join(stock_dir, filename))
        except pd.errors.EmptyDataError:
            logger.info("%s:empty data" % code)
            continue
        df.columns = ['cdate', 'open', 'high', 'low', 'close', 'volume', 'amount']
        #logger.info("readcsv :%s" % filename)
        if not stock.mysql_client.set(df, d_table_name):
            logger.info("failed :%s" % filename)
        succeed_list.append(filename)
        logger.info("succeed :%s" % filename)

if __name__ == "__main__":
    read_stock_csv("/data/tdx")
