#coding=utf-8
import _pickle
import const as ct
import pandas as pd
from log import getLogger
from datetime import datetime
from ccalendar import CCalendar
from combination import Combination
from common import float_random, get_market 
logger = getLogger(__name__)
class CIndex(Combination):
    ZZ_URL_HEAD        = 'http://www.csindex.com.cn/uploads/file/autofile/cons/%scons.xls'
    ZZ_URL_WEIGHT_HEAD = 'http://www.csindex.com.cn/uploads/file/autofile/closeweight/%scloseweight.xls'
    SZ_URL_HEAD        = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1747_zs&TABKEY=tab1&ZSDM=%s&random=%s'
    INDEX_URLS = {
        '000001': (ZZ_URL_HEAD % '000001', [0, 4, 5], ['date', 'code', 'name']),
        '000016': (ZZ_URL_HEAD % '000016', [0, 4, 5], ['date', 'code', 'name']),
        '000300': (ZZ_URL_WEIGHT_HEAD % '000300', [0, 4, 5, 8], ['date', 'code', 'name', 'weight']),
        '000905': (ZZ_URL_WEIGHT_HEAD % '000905', [0, 4, 5, 8], ['date', 'code', 'name', 'weight']),
        '399001': (SZ_URL_HEAD % ('399001', float_random()), [0,1,5], ['code', 'name', 'weight']),
        '399005': (SZ_URL_HEAD % ('399005', float_random()), [0,1,5], ['code', 'name', 'weight']),
        '399006': (SZ_URL_HEAD % ('399006', float_random()), [0,1,5], ['code', 'name', 'weight']),
        '399673': (SZ_URL_HEAD % ('399673', float_random()), [0,1,5], ['code', 'name', 'weight'])
    }

    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None):
        Combination.__init__(self, code, dbinfo, redis_host)
        if not self.create_mysql_table():
            raise Exception("create index %s table failed" % self.code)

    @staticmethod
    def get_dbname(code):
        return "i%s" % code

    def run(self, data):
        if not data.empty:
            self.redis.set(self.get_redis_name(self.get_dbname(self.code)), _pickle.dumps(data.tail(1), 2))
            self.influx_client.set(data)

    def get_market(self):
        if self.code.startswith("000") or self.code.startswith("880"):
            return ct.MARKET_SH
        elif self.code.startswith("399"):
            return ct.MARKET_SZ
        else:
            return ct.MARKET_OTHER

    def create_mysql_table(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                if table_name == 'day':
                    sql = 'create table if not exists %s(date varchar(10),\
                                                         open float,\
                                                         high float,\
                                                         close float,\
                                                         preclose float,\
                                                         low float,\
                                                         volume float,\
                                                         amount float,\
                                                         preamount float,\
                                                         pchange float,\
                                                         mchange float,\
                                                         PRIMARY KEY(date))' % table_name
                if not self.mysql_client.create(sql, table_name): return False
        return True

    def get_k_data_in_range(self, start_date, end_date):
        table_name = 'day'
        sql = "select * from %s where date between \"%s\" and \"%s\"" %(table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, date = None):
        table_name = 'day'
        if date is not None:
            sql = "select * from %s where date=\"%s\"" % (table_name, date)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    def create_components_table(self, table_name):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(20) not null,\
                                             name varchar(20),\
                                             weight float,\
                                             flag int,\
                                             PRIMARY KEY (date, code))' % table_name
        return True if table_name in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table_name)

    def get_components_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        sql = "select * from %s where date=\"%s\"" % (self.get_components_table_name(cdate), cdate)
        return self.mysql_client.get(sql)

    def set_components_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        table_name = self.get_components_table_name(cdate)
        if not self.is_table_exists(table_name):
            if not self.create_components_table(table_name):
                logger.error("create components table failed")
                return False

        if self.is_date_exists(table_name, cdate): 
            logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return True

        url          = self.INDEX_URLS[self.code][0]
        columns      = self.INDEX_URLS[self.code][1]
        column_names = self.INDEX_URLS[self.code][2]
        df           = pd.read_excel(url, usecols = columns)
        df.columns   = column_names
        df.code      = df.code.astype('str').str.zfill(6)
        df['date']   = cdate
        if 'wieight' not in df.columns:
            df['weight'] = 1/len(df)
        if 'flag' not in df.columns:
            df['flag']   = 1
        df = df.reset_index(drop = True)

        if self.mysql_client.set(df, table_name):
            self.redis.sadd(table_name, cdate)
            return True
        return False

    def get_components_table_name(self, cdate):
        cdates = cdate.split('-')
        return "%s_components_%s_%s" % (self.get_dbname(self.code), cdates[0], (int(cdates[1])-1)//3 + 1)

    def set_k_data(self, fpath = "/data/tdx/history/days/%s"):
        prestr = "1" if self.get_market() == ct.MARKET_SH else "0"
        filename = "%s%s.csv" % (prestr, self.code)
        df = pd.read_csv(fpath % filename, sep = ',')
        df = df[['date', 'open', 'high', 'close', 'low', 'amount', 'volume']]
        df['date'] = df['date'].astype(str)
        df['date'] = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")

        df['preclose'] = df['close'].shift(1)
        df.at[0, 'preclose'] = df.loc[0, 'open']
        df['pchange'] = 100 * (df['close'] - df['preclose']) / df['preclose']

        df['preamount'] = df['amount'].shift(1)
        df.at[0, 'preamount'] = df.loc[0, 'amount']
        df['mchange'] = 100 * (df['amount'] - df['preamount']) / df['preamount']

        df = df.reset_index(drop = True)
        return self.mysql_client.set(df, 'day', method = ct.REPLACE)

if __name__ == '__main__':
    for code in ["000001", "000300", "000016", "000905", "399673", "399001", "399005", "399006"]:
        av   = CIndex(code)
        res  = av.set_components_data()
        data = av.get_components_data()
        print("code:%s, length:%s" % (code, len(data)))
