#coding=utf-8
import _pickle
import const as ct
import pandas as pd
from combination import Combination
from log import getLogger
logger = getLogger(__name__)
class CIndex(Combination):
    def __init__(self, dbinfo, code):
        Combination.__init__(self, dbinfo, code)

    @staticmethod
    def get_dbname(code):
        return "i%s" % code

    def run(self, data):
        if not data.empty:
            self.redis.set(self.get_redis_name(self.get_dbname(self.code)), _pickle.dumps(data, 2))
            self.influx_client.set(data)

    def get_market(self):
        if self.code.startswith("0") or self.code.startswith("880"):
            return ct.MARKET_SH
        elif self.code.startswith("399"):
            return ct.MARKET_SZ
        else:
            return ct.MARKET_OTHER

    def get_k_data_in_range(self, start_date, end_date):
        table_name = 'day'
        sql = "select * from %s where cdate between \"%s\" and \"%s\"" %(table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, date = None):
        table_name = 'day'
        if date is not None:
            sql = "select * from %s where cdate=\"%s\"" % (table_name, date)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    def set_k_data(self):
        prestr = "1" if self.get_market() == ct.MARKET_SH else "0"
        filename = "%s%s.csv" % (prestr, self.code)
        df = pd.read_csv("/data/tdx/history/days/%s" % filename, sep = ',')
        df = df[['date', 'open', 'high', 'close', 'low', 'amount', 'volume']]
        df['date'] = df['date'].astype(str)
        df['date'] = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")
        df = df.rename(columns={'date':'cdate'})
        df = df.reset_index(drop = True)
        return self.mysql_client.set(df, 'day', method = ct.REPLACE)

if __name__ == '__main__':
    av = CIndex(ct.DB_INFO, '000001')
    data = av.get_k_data()
    print(data)
