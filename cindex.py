#coding=utf-8
import os
import _pickle
import const as ct
import pandas as pd
from cinfluxdb import CInflux
from datetime import datetime
from base.cobj import CMysqlObj
from base.clog import getLogger
from pytdx.reader import BlockReader
from base.cdate import transfer_date_string_to_int
# http://blog.sina.com.cn/s/blog_623d2d280102vt8y.html
from common import float_random, is_df_has_unexpected_data, smart_get
logger = getLogger(__name__)
class CIndex(CMysqlObj):
    ZZ_URL_HEAD        = 'http://www.csindex.com.cn/uploads/file/autofile/cons/%scons.xls'
    SZ_URL_HEAD        = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1747_zs&TABKEY=tab1&ZSDM=%s&random=%s'
    ZZ_URL_WEIGHT_HEAD = 'http://www.csindex.com.cn/uploads/file/autofile/closeweight/%scloseweight.xls'
    INDEX_URLS = {
        '000001': (ZZ_URL_HEAD % '000001', [0, 4, 5], ['date', 'code', 'name']),
        '000016': (ZZ_URL_HEAD % '000016', [0, 4, 5], ['date', 'code', 'name']),
        '000300': (ZZ_URL_WEIGHT_HEAD % '000300', [0, 4, 5, 8], ['date', 'code', 'name', 'weight']),
        '000905': (ZZ_URL_WEIGHT_HEAD % '000905', [0, 4, 5, 8], ['date', 'code', 'name', 'weight']),
        '399001': (SZ_URL_HEAD % ('399001', float_random()), [0, 1, 5], ['code', 'name', 'weight']),
        '399005': (SZ_URL_HEAD % ('399005', float_random()), [0, 1, 5], ['code', 'name', 'weight']),
        '399006': (SZ_URL_HEAD % ('399006', float_random()), [0, 1, 5], ['code', 'name', 'weight']),
        '399673': (SZ_URL_HEAD % ('399673', float_random()), [0, 1, 5], ['code', 'name', 'weight'])
    }

    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None, should_create_influxdb = False, should_create_mysqldb = False):
        super(CIndex, self).__init__(code, self.get_dbname(code), dbinfo, redis_host)
        self.code = code
        self.influx_client = CInflux(ct.IN_DB_INFO, self.dbname, iredis = self.redis)
        #self.mysql_client.delete_db(self.dbname)
        if not self.create(should_create_influxdb, should_create_mysqldb):
            raise Exception("create index %s table failed" % self.code)

    @staticmethod
    def get_dbname(code):
        return "i%s" % code

    @staticmethod
    def get_redis_name(code):
        return "realtime_i%s" % code

    @staticmethod
    def get_market(code):
        if code.startswith("000") or code.startswith("880"):
            return ct.MARKET_SH
        elif code.startswith("399"):
            return ct.MARKET_SZ
        else:
            return ct.MARKET_OTHER

    def run(self, data):
        if not data.empty:
            self.redis.set(self.get_redis_name(self.dbname), _pickle.dumps(data.tail(1), 2))
            self.influx_client.set(data)

    def get_day_table(self):
        return "%s_day" % self.dbname

    def create_influx_db(self):
        return self.influx_client.create()

    def create(self, should_create_influxdb, should_create_mysqldb):
        influxdb_flag = self.create_influx_db() if should_create_influxdb else True
        mysql_flag = self.create_db(self.dbname) and self.create_mysql_table() if should_create_mysqldb else True
        return influxdb_flag and mysql_flag

    def create_mysql_table(self):
        table_name = self.get_day_table()
        if table_name not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10),\
                                                 open float,\
                                                 high float,\
                                                 close float,\
                                                 preclose float,\
                                                 low float,\
                                                 volume bigint,\
                                                 amount float,\
                                                 preamount float,\
                                                 pchange float,\
                                                 mchange float,\
                                                 PRIMARY KEY(date))' % table_name
            if not self.mysql_client.create(sql, table_name): return False
        return True

    def get_components_table_name(self, cdate):
        cdates = cdate.split('-')
        return "%s_components_%s_%s" % (self.dbname, cdates[0], (int(cdates[1])-1)//3 + 1)

    def get_stock_day_table(self):
        return "%s_day" % self.dbname

    def get_k_data_in_range(self, start_date, end_date):
        table_name = self.get_stock_day_table()
        sql = "select * from %s where date between \"%s\" and \"%s\"" %(table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, date = None):
        table_name = self.get_stock_day_table()
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
        table_name = self.get_components_table_name(cdate)
        if not self.is_table_exists(table_name): return pd.DataFrame()
        sql = "select * from %s where date=\"%s\"" % (table_name, cdate)
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

        df = smart_get(pd.read_excel, url, usecols = columns)
        if df is None:
            logger.error("data for %s is empty" % self.code)
            return False
        df.columns   = column_names
        df.code      = df.code.astype('str').str.zfill(6)
        df['date']   = cdate
        if 'wieight' not in df.columns:
            df['weight'] = 1/len(df)
        if 'flag' not in df.columns:
            df['flag']   = 1
        if self.code == "000001": df = df[df.code.str.startswith('6')]
        df = df.reset_index(drop = True)

        if is_df_has_unexpected_data(df):
            logger.error("data for %s is not clear" % self.code)
            return False

        if self.mysql_client.set(df, table_name):
            if self.redis.sadd(table_name, cdate): return True
        return False

    def handle_unexpected_data(self, df):
        if self.code == '399005':
            df.loc[df.date == '2007-06-20', 'amount'] = 7.285928e+09 
        return df

    def read(self, fpath):
        prestr = "1" if self.get_market(self.code) == ct.MARKET_SH else "0"
        filename = "%s%s.csv" % (prestr, self.code)
        dheaders = ['date', 'open', 'high', 'close', 'low', 'amount', 'volume']
        df = pd.read_csv(fpath % filename, sep = ',', usecols = dheaders)
        df['date'] = df['date'].astype(str)
        df['date'] = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")
        df = self.handle_unexpected_data(df)
        df = df.reset_index(drop = True)
        df = df.sort_values(by = 'date', ascending= True)
        return df

    def set_k_data(self, cdate = None, fpath = "/data/tdx/history/days/%s"):
        if cdate is None:
            return self.set_all_data(fpath)
        else:
            return self.set_day_data(cdate, fpath)

    def set_all_data(self, fpath):
        df = self.read(fpath)
        if 0 == len(df):
            logger.error("no data for %s" % self.code)
            return False
        else:
            if 1 == len(df):
                df.at[0, 'preclose']  = df.loc[0, 'open']
                df.at[0, 'pchange']   = 100 * (df.at[0, 'close'] - df.at[0, 'preclose']) / df.at[0, 'preclose']
                df.at[0, 'preamount'] = df.loc[0, 'amount']
                df.at[0, 'mchange']   = 0
            else:
                df['preclose'] = df['close'].shift(1)
                df.at[0, 'preclose'] = df.loc[0, 'open']
                df['pchange'] = 100 * (df['close'] - df['preclose']) / df['preclose']
                df['preamount'] = df['amount'].shift(1)
                df.at[0, 'preamount'] = df.loc[0, 'amount']
                df['mchange'] = 100 * (df['amount'] - df['preamount']) / df['preamount']

        day_table = self.get_day_table()
        existed_date_list = self.get_existed_keys_list(day_table)
        df = df[~df.date.isin(existed_date_list)]
        df = df.reset_index(drop = True)
        if df.empty: return True

        if is_df_has_unexpected_data(df):
            logger.error("data for %s is not clear" % self.code)
            return False

        if not self.mysql_client.set(df, day_table):
            logger.error("set data %s %s failed" % (self.code, day_table))
            return False

        if 0 == self.redis.sadd(day_table, *set(df.date.tolist())):
            logger.error("sadd %s for %s failed" % (self.code, day_table))
            return False
        return True

    def set_day_data(self, cdate, fpath):
        day_table = self.get_day_table()
        if self.is_date_exists(day_table, cdate): 
            logger.debug("existed data for code:%s, date:%s" % (self.code, cdate))
            return True

        df = self.read(fpath)
        if 0 == len(df):
            logger.error("no data for %s:%s" % (cdate, self.code))
            return False
        elif 1 == len(df):
            if len(df.loc[df.date == cdate]) == 0:
                logger.error("no data:%s for %s" % (cdate, self.code))
                return False
            df.at[0, 'preclose']  = df.at[0, 'open']
            df.at[0, 'pchange']   = 100 * (df.at[0, 'close'] - df.at[0, 'preclose']) / df.at[0, 'preclose']
            df.at[0, 'preamount'] = df.at[0, 'amount']
            df.at[0, 'mchange']   = 0
        else:
            index_list = df.loc[df.date == cdate].index.values
            if len(index_list) == 0: 
                logger.error("no data:%s for %s" % (cdate, self.code))
                return False
            preday_index = index_list[0] - 1
            preday_df = df.loc[preday_index]
            df = df.loc[df.date == cdate]
            df['preclose'] = preday_df['close']
            df['pchange'] = 100 * (df['close'] - df['preclose']) / df['preclose']
            df['preamount'] = preday_df['amount']
            df['mchange'] = 100 * (df['amount'] - df['preamount']) / df['preamount']

        df = df.reset_index(drop = True)
        day_table = self.get_day_table()
        if is_df_has_unexpected_data(df):
            logger.error("data for %s is not clear" % self.code)
            return False

        if self.mysql_client.set(df, day_table):
            if self.redis.sadd(day_table, cdate):
                return True
        return False

    def get_val_filename(self):
        return "%s_val.csv" % self.dbname

    def get_val_data(self, mdate = None):
        index_val_path = os.path.join("/data/valuation/indexs", self.get_val_filename())
        if not os.path.exists(index_val_path): return None
        df = pd.read_csv(index_val_path)
        if mdate is None:
            return df
        else:
            return df.loc[df.date == mdate].reset_index(drop = True)

    def set_val_data(self, df, mdate = '', fpath = "/data/valuation/indexs"):
        index_val_path = os.path.join(fpath, self.get_val_filename())
        if mdate == '':
            df.to_csv(index_val_path, index=False, header=True, mode='w', encoding='utf8')
        else:
            if not os.path.exists(index_val_path):
                df.to_csv(index_val_path, index=False, header=True, mode='w', encoding='utf8')
            else:
                vdf = self.get_val_data(mdate)
                if vdf.empty:
                    df.to_csv(index_val_path, index=False, header=False, mode='a+', encoding='utf8')
        return True

class TdxFgIndex(CIndex):
    #通达信风格指数
    def __init__(self, code, dbinfo = ct.DB_INFO, redis_host = None, should_create_influxdb = False, should_create_mysqldb = False):
        self.name = self.get_fg_index_name(code)
        super(TdxFgIndex, self).__init__(code, dbinfo, redis_host, should_create_influxdb, should_create_mysqldb)
       
    def get_fg_index_name(self, tcode, fname = ct.TONG_DA_XIN_INDEX_PATH):
        with open(fname, "rb") as f:
            data = f.read()
        str_list = data.decode("gb2312").split('\r\n')
        for x in str_list:
            info_list = x.split('|')
            name = info_list[0]
            ccode = info_list[1]
            if ccode == tcode: return name
        raise Exception("can not find index name for %s" % self.code)

    def get_fg_index_df(self, cdate):
        df = BlockReader().get_df(ct.TONG_DA_XIN_FG_INDEX_PATH)
        df = df.loc[df.blockname == self.name]
        df = df[['code']]
        df['date'] = cdate
        df = df.reset_index(drop = True)
        return df

    def create_components_table(self, table_name):
        sql = 'create table if not exists %s(date varchar(10) not null,\
                                             code varchar(20) not null,\
                                             PRIMARY KEY (date, code))' % table_name
        return True if table_name in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table_name)

    def set_components_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        table_name = self.get_components_table_name(cdate)
        if not self.is_table_exists(table_name):
            if not self.create_components_table(table_name):
                logger.error("create components table failed")
                return False

        if self.is_date_exists(table_name, cdate): 
            logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return True

        df = self.get_fg_index_df(cdate)

        if is_df_has_unexpected_data(df):
            logger.error("data for %s is not clear" % self.code)
            return False

        if self.mysql_client.set(df, table_name):
            if self.redis.sadd(table_name, cdate): return True
        return False

if __name__ == '__main__':
    tdi = TdxFgIndex(code = '880883', should_create_influxdb = True, should_create_mysqldb = True)
    tdi.set_k_data()
    #for code in ["000001", "000300", "000016", "000905", "399673", "399001", "399005", "399006"]:
    #    av   = CIndex(code)
    #    res  = av.set_components_data()
    #    data = av.get_components_data()
    #    print("code:%s, length:%s" % (code, len(data)))
