#coding=utf-8
import _pickle
import const as ct
import pandas as pd
from combination import Combination
from log import getLogger
logger = getLogger(__name__)
class CIndex(Combination):
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
        if self.code.startswith("0") or self.code.startswith("880"):
            return ct.MARKET_SH
        elif self.code.startswith("399"):
            return ct.MARKET_SZ
        else:
            return ct.MARKET_OTHER

    def create_mysql_table(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(date varchar(10), open float, high float, close float, preclose float, low float, volume float, amount float, preamount float, pchange float, mchange float, PRIMARY KEY(date))' % table_name
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
    av = CIndex('000001')
    data = av.set_k_data()
    print(data)
