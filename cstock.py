#coding=utf-8
import os
import time
import _pickle
import datetime
import const as ct
import pandas as pd
import tushare as ts
from chip import Chip
from features import Mac
from cmysql import CMySQL
from log import getLogger
from ticks import read_tick
from cinfluxdb import CInflux
from datetime import datetime
from common import create_redis_obj, get_years_between
from futuquant.quote.quote_response_handler import TickerHandlerBase
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
logger = getLogger(__name__)

class CStock(TickerHandlerBase):
    def __init__(self, code, dbinfo = ct.DB_INFO, should_create_influxdb = False, should_create_mysqldb = True):
        self.code = code
        self.dbname = self.get_dbname(code)
        self.redis = create_redis_obj()
        self.data_type_dict = {9:"day"}
        self.chip_client = Chip()
        self.influx_client = CInflux(ct.IN_DB_INFO, dbname = self.dbname, iredis = self.redis)
        self.mysql_client = CMySQL(dbinfo, dbname = self.dbname, iredis = self.redis)
        if not self.create(should_create_influxdb, should_create_mysqldb):
            raise Exception("create stock %s table failed" % self.code)

    def __del__(self):
        self.redis = None
        self.influx_client = None
        self.mysql_client = None
        self.data_type_dict = None

    @staticmethod
    def get_dbname(code):
        return "s%s" % code

    @staticmethod
    def get_redis_name(code):
        return "realtime_%s" % code

    def on_recv_rsp(self, rsp_pb):
        '''获取逐笔 get_rt_ticker 和 TickerHandlerBase'''
        ret, data = super(CStock, self).on_recv_rsp(rsp_pb)
        return ret, data

    def adjust_share(self, data, info):
        data['outstanding'] = 0
        data['totals'] = 0
        if 0 == len(info): return data
        end_index = 0
        pre_totals = 0
        pre_outstanding = 0
        last_pre_totals = 0
        last_pre_outstanding = 0
        for info_index, start_date in info.date.iteritems():
            dates = data.loc[data.date >= start_date].index.tolist()
            if len(dates) == 0 : continue
            start_index = end_index
            end_index = dates[len(dates) - 1]
    
            pre_outstanding = int(info.loc[info_index, 'money'])   #前流通盘
            pre_totals = int(info.loc[info_index, 'price'])   #前总股本
            cur_outstanding = int(info.loc[info_index, 'count'])   #后流通盘
            cur_totals = int(info.loc[info_index, 'rate'])    #后总股本
    
            if 0 == info_index:
                data.at[start_index:end_index, 'outstanding'] = cur_outstanding
                data.at[start_index:end_index, 'totals'] = cur_totals
                last_pre_outstanding = pre_outstanding
                last_pre_totals = pre_totals
            else:
                #if cur_outstanding != last_pre_outstanding:
                #   logger.debug("%s 日期:%s 前流通盘:%s 不等于 预期前流通盘:%s" % (self.code, start_date, cur_outstanding, last_pre_outstanding))
                #elif cur_totals != last_pre_totals:
                #   logger.debug("%s 日期:%s 后流通盘:%s 不等于 预期后流通盘:%s" % (self.code, start_date, cur_totals, last_pre_totals))
                data.at[start_index + 1:end_index, 'outstanding'] = cur_outstanding
                data.at[start_index + 1:end_index, 'totals'] = cur_totals
                last_pre_outstanding = pre_outstanding
                last_pre_totals = pre_totals
    
                #finish the last date
                if info_index == len(info) - 1:
                    data.at[end_index + 1:, 'outstanding'] = last_pre_outstanding
                    data.at[end_index + 1:, 'totals'] = last_pre_totals
        return data
    
    def qfq(self, data, info):
        data['adj'] = 1.0
        data['preclose'] = data['close'].shift(-1)
        if 0 == len(info): return data
        for info_index, start_date in info.date.iteritems():
            dates = data.loc[data.date >= start_date].index.tolist()
            if len(dates) == 0 : continue
            rate  = info.loc[info_index, 'rate']    #配k股
            price = info.loc[info_index, 'price']   #配股价格
            money = info.loc[info_index, 'money']   #分红
            count = info.loc[info_index, 'count']   #转送股数量
            start_index = dates[len(dates) - 1]
            adj = (data.loc[start_index, 'preclose'] * 10 - money + rate * price) / ((10 + rate + count) * data.loc[start_index, 'preclose'])
            data.at[start_index + 1:, 'adj'] = data.loc[start_index + 1:, 'adj'] * adj
        return data

    def has_on_market(self, cdate):
        time2Market = self.get('timeToMarket')
        if str(time2Market) == '0': return False
        t = time.strptime(str(time2Market), "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)

        t = time.strptime(cdate, "%Y-%m-%d")
        y,m,d = t[0:3]
        time4Date = datetime(y,m,d)
        return True if (time4Date - time2Market).days > 0 else False

    def is_subnew(self, time2Market = None, timeLimit = 365):
        if time2Market == '0': return False #for stock has not been in market
        if time2Market == None: time2Market = self.get('timeToMarket')
        t = time.strptime(time2Market, "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)
        return True if (datetime.today()-time2Market).days < timeLimit else False

    def create(self, should_create_influxdb, should_create_mysqldb):
        influxdb_flag = self.create_influx_db() if should_create_influxdb else True
        mysqldb_flag = self.create_mysql_table() if should_create_mysqldb else True
        return influxdb_flag and mysqldb_flag

    def create_influx_db(self):
        return self.influx_client.create()

    def create_mysql_table(self):
        for _, table_name in self.data_type_dict.items():
            if table_name not in self.mysql_client.get_all_tables():
                sql = 'create table if not exists %s(cdate varchar(10) not null, open float, high float, close float, low float, volume float, amount float, outstanding float, totals float, adj float, PRIMARY KEY(cdate))' % table_name 
                if not self.mysql_client.create(sql, table_name): return False
        return True

    def create_ticket_table(self, table):
        sql = 'create table if not exists %s(date varchar(10) not null, ctime varchar(8) not null, price float(5,2), cchange varchar(10) not null, volume int not null, amount int not null, ctype varchar(9) not null, PRIMARY KEY (date, ctime, cchange, volume, amount, ctype))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def get(self, attribute):
        df_byte = self.redis.get(ct.STOCK_INFO)
        if df_byte is None: return None
        df = _pickle.loads(df_byte)
        if len(df.loc[df.code == self.code][attribute].values) == 0: return None
        return df.loc[df.code == self.code][attribute].values[0]

    def run(self, data):
        self.redis.set(self.get_redis_name(self.code), _pickle.dumps(data.tail(1), 2))
        self.influx_client.set(data)

    def merge_ticket(self, df):
        ex = df[df.duplicated(subset = ['ctime', 'cchange', 'volume', 'amount', 'ctype'], keep=False)]
        dlist = list(ex.index)
        while len(dlist) > 0:
            snum = 1
            sindex = dlist[0]
            for _index in range(1, len(dlist)):
                if sindex + 1 == dlist[_index]: 
                    snum += 1
                    if _index == len(dlist) -1:
                        df.drop_duplicates(keep='first', inplace=True)
                        df.at[sindex, 'volume'] = snum * df.loc[sindex]['volume']
                        df.at[sindex, 'amount'] = snum * df.loc[sindex]['amount']
                else:
                    df.drop_duplicates(keep='first', inplace=True)
                    df.at[sindex, 'volume'] = snum * df.loc[sindex]['volume']
                    df.at[sindex, 'amount'] = snum * df.loc[sindex]['amount']
                    sindex = dlist[_index]
                    snum = 1
            df = df.reset_index(drop = True)
            ex = df[df.duplicated(subset = ['ctime', 'cchange', 'volume', 'amount', 'ctype'], keep=False)]
            dlist = list(ex.index)
        return df

    def get_market(self):
        if self.code.startswith("6") or self.code.startswith("500") or self.code.startswith("550") or self.code.startswith("510") or self.code.startswith("7"):
            return ct.MARKET_SH
        elif self.code.startswith("00") or self.code.startswith("30") or self.code.startswith("150") or self.code.startswith("159"):
            return ct.MARKET_SZ
        else:
            return ct.MARKET_OTHER

    def get_chip_distribution_table(self, cdate):
        cdates = cdate.split('-')
        return "chip_%s_%s" % (self.code, cdates[0])

    def get_redis_tick_table(self, cdate):
        cdates = cdate.split('-')
        return "tick_%s_%s_%s" % (self.code, cdates[0], cdates[1])

    def create_chip_table(self, table):
        sql = 'create table if not exists %s(pos int not null, sdate varchar(10) not null, date varchar(10) not null, price decimal(8,2) not null, volume int not null, outstanding int not null, PRIMARY KEY (pos, sdate, date, price, volume, outstanding))' % table
        return True if table in self.mysql_client.get_all_tables() else self.mysql_client.create(sql, table)

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return table_name in set(str(table, encoding = "utf8") for table in self.redis.smembers(self.dbname))
        return False

    def is_date_exists(self, tick_table, cdate):
        if self.redis.exists(tick_table):
            return cdate in set(str(tdate, encoding = "utf8") for tdate in self.redis.smembers(tick_table))
        return False

    def get_pre_str(self):
        return "1" if self.get_market() == ct.MARKET_SH else "0"

    def read_file(self):
        prestr = self.get_pre_str()
        filename = "/data/tdx/history/days/%s%s.csv" % (prestr, self.code)
        if not os.path.exists(filename): 
            return pd.DataFrame()
        df = pd.read_csv(filename, sep = ',')
        df = df[['date', 'open', 'high', 'close', 'low', 'amount', 'volume']]
        return df

    def collect_right_info(self, info):
        info = info[(info.code == self.code) & (info.date <= int(datetime.now().strftime('%Y%m%d')))]
        info = info.sort_values(by = 'date' , ascending = False)
        info = info.reset_index(drop = True)

        #collect stock amount change info
        total_stock_change_type_list = ['2', '3', '4', '5', '7', '8', '9', '10', '11']
        s_info = info[info.type.isin(total_stock_change_type_list)]
        s_info = s_info[['date', 'type', 'money', 'price', 'count', 'rate']] 
        s_info = s_info.sort_index(ascending = True)
        s_info = s_info.reset_index(drop = True)

        #collect stock price change info
        t_info = info[info.type == 1]
        t_info = t_info[['money', 'price', 'count', 'rate', 'date']]
        t_info = t_info.sort_index(ascending = True)
        t_info = t_info.reset_index(drop = True)
        return s_info, t_info

    def transfer_2_adjusted(self, df):
        df = df[['date', 'open', 'high', 'close', 'low', 'volume', 'amount', 'outstanding', 'totals', 'adj']]

        df['date'] = df['date'].astype(str)
        df['date'] = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")
        df = df.rename(columns={'date':'cdate'})

        df['low']    = df['adj'] * df['low']
        df['open']   = df['adj'] * df['open']
        df['high']   = df['adj'] * df['high']
        df['close']  = df['adj'] * df['close']
        df['volume'] = df['volume'].astype(int)
        df['aprice'] = df['adj'] * df['amount'] / df['volume']
        df['totals'] = df['totals'].astype(int)
        df['totals'] = df['totals'] * 10000
        df['outstanding'] = df['outstanding'].astype(int)
        df['outstanding'] = df['outstanding'] * 10000
        return df

    def set_k_data(self, info, cdate = None):
        cdate = datetime.now().strftime('%Y-%m-%d') if cdate is None else cdate

        #if not on market, just return True
        if not self.has_on_market(cdate): 
            return True

        df = self.read_file()
        if df.empty:
            logger.error("read empty file for:%s" % self.code)
            return False

        df = df.sort_values(by = 'date', ascending= False)
        df = df.reset_index(drop = True)

        now_time = str(df.head(1).date[0])
        now_time = "%s-%s-%s" % (now_time[0:4], now_time[4:6], now_time[6:8])
        if now_time != cdate:
            logger.error("data new date %s is not equal to now date %s" % (now_time, cdate))
            return False

        s_info, t_info = self.collect_right_info(info)

        df = self.adjust_share(df, s_info)

        df = self.qfq(df, t_info)

        #transfer data to split-adjusted share prices
        df = self.transfer_2_adjusted(df)

        #get ulimit average price
        df['uprice'] = Mac(df.aprice, 0)

        #get 60 day average price
        df['60price'] = Mac(df.aprice, 60)

        df = df.sort_values(by = 'cdate', ascending= True)
        df = df.reset_index(drop = True)

        #set k data
        write_kdata_flag = self.mysql_client.set(df, 'day', method = ct.REPLACE)

        #set chip distribution
        write_chip_flag = self.set_chip_distribution(df.tail(2), cdate)

        return write_kdata_flag and write_chip_flag

    def get_chip_distribution(self, mdate = None):
        df = pd.DataFrame()
        if mdate is not None:
            table = self.get_chip_distribution_table(mdate)
            if self.is_table_exists(table):
                df = self.mysql_client.get("select * from %s" % table)
                return df.loc[df.date == mdate]
        else:
            for table in [self.get_chip_distribution_table(myear) for myear in year_list]:
                if self.is_table_exists(table):
                    tmp_df = self.mysql_client.get("select * from %s" % table)
                    df = df.append(tmp_df)
        return df

    def set_chip_distribution(self, data, zdate = None):
        data = data[['cdate', 'open', 'aprice', 'outstanding', 'volume', 'amount']]
        if zdate is None:
            df = self.chip_client.compute_distribution(data)
            time2Market = self.get('timeToMarket')
            start_year = int(time2Market / 10000)
            end_year = int(datetime.now().strftime('%Y'))
            year_list = get_years_between(start_year, end_year)
            res_flag = True
            for myear in year_list:
                chip_table = self.get_chip_distribution_table(myear)
                if not self.is_table_exists(chip_table):
                    if not self.create_chip_table(chip_table):
                        logger.error("create chip table:%s failed" % chip_table)
                        return False
                    self.redis.sadd(self.dbname, chip_table)
                tmp_df = df[df.sdate.str.split('-', expand = True)[0] == myear]
                tmp_df = tmp_df.reset_index(drop = True)
                if not self.mysql_client.set(tmp_df, chip_table, method = ct.REPLACE):
                    logger.error("%s set data for %s failed" % (self.code, myear))
                    res_flag = False
                else:
                    for cdate in tmp_df.date:
                        self.redis.sadd(chip_table, cdate)
            return res_flag
        else:
            chip_table = self.get_chip_distribution_table(zdate)
            if self.is_date_exists(chip_table, zdate): 
                logger.debug("existed chip for code:%s, date:%s" % (self.code, cdate))
                return True

            mdate_list = data.cdate.tolist()
            pre_date = mdate_list[0]
            now_date = mdate_list[1]
            if now_date != zdate:
                logger.error("data new date %s is not equal to now date %s" % (now_date, zdate))
                return False

            pre_date_dist = self.get_chip_distribution(pre_date)
            pre_date_dist = pre_date_dist.sort_values(by = 'pos', ascending= True)

            pos = data.loc[data.cdate == zdate].index[0]
            volume = data.loc[data.cdate == zdate, 'volume'].tolist()[0]
            aprice = data.loc[data.cdate == zdate, 'aprice'].tolist()[0]
            outstanding = data.loc[data.cdate == zdate, 'outstanding'].tolist()[0]
            pre_outstanding = data.loc[data.cdate == pre_date, 'outstanding'].tolist()[0]

            tmp_df = self.chip_client.adjust_volume(pre_date_dist, pos, volume, pre_outstanding, outstanding)
            tmp_df.date = zdate
            tmp_df.outstanding = outstanding
            tmp_df = tmp_df.append(pd.DataFrame([[pos, now_date, now_date, aprice, volume, outstanding]], columns = ct.CHIP_COLUMNS))
            tmp_df = tmp_df[tmp_df.volume != 0]
            tmp_df = tmp_df.reset_index(drop = True)

            if not self.is_table_exists(chip_table):
                if not self.create_chip_table(chip_table):
                    logger.error("create chip table failed")
                    return False
                self.redis.sadd(self.dbname, chip_table)

            if self.mysql_client.set(tmp_df, chip_table): 
                self.redis.sadd(chip_table, zdate)
                logger.debug("finish record chip:%s. table:%s" % (self.code, chip_table))
                return True
            return False

    def set_ticket(self, cdate = None):
        cdate = datetime.now().strftime('%Y-%m-%d') if cdate is None else cdate
        if not self.has_on_market(cdate):
            logger.debug("not on market code:%s, date:%s" % (self.code, cdate))
            return True
        tick_table = self.get_redis_tick_table(cdate)
        if not self.is_table_exists(tick_table):
            if not self.create_ticket_table(tick_table):
                logger.error("create tick table failed")
                return False
            self.redis.sadd(self.dbname, tick_table)
        if self.is_date_exists(tick_table, cdate): 
            logger.debug("existed code:%s, date:%s" % (self.code, cdate))
            return True
        logger.debug("%s read code from file %s" % (self.code, cdate))
        df = ts.get_tick_data(self.code, date=cdate)
        df_tdx = read_tick(os.path.join(ct.TIC_DIR, '%s.tic' % datetime.strptime(cdate, "%Y-%m-%d").strftime("%Y%m%d")), self.code)
        if not df_tdx.empty:
            if df is not None and not df.empty and df.loc[0]['time'].find("当天没有数据") == -1:
                net_volume = df.volume.sum()
                tdx_volume = df_tdx.volume.sum()
                if net_volume != tdx_volume:
                    logger.error("code:%s, date:%s, net volume:%s, tdx volume:%s not equal" % (self.code, cdate, net_volume, tdx_volume))
            df = df_tdx
        else:
            if df is None:
                logger.debug("nonedata code:%s, date:%s" % (self.code, cdate))
                return True
            if df.empty:
                logger.debug("emptydata code:%s, date:%s" % (self.code, cdate))
                return True
            if df.loc[0]['time'].find("当天没有数据") != -1:
                logger.debug("nodata code:%s, date:%s" % (self.code, cdate))
                return True
        df.columns = ['ctime', 'price', 'cchange', 'volume', 'amount', 'ctype']
        logger.debug("merge ticket code:%s date:%s" % (self.code, cdate))
        df = self.merge_ticket(df)
        df['date'] = cdate
        logger.debug("write data code:%s, date:%s, table:%s" % (self.code, cdate, tick_table))
        if self.mysql_client.set(df, tick_table):
            logger.debug("finish record:%s. table:%s" % (self.code, tick_table))
            self.redis.sadd(tick_table, cdate)
            return True
        return False

    def get_ticket(self, cdate):
        cdate = datetime.now().strftime('%Y-%m-%d') if cdate is None else cdate
        if not self.has_on_market(cdate):
            logger.debug("not on market code:%s, date:%s" % (self.code, cdate))
            return
        sql = "select * from %s where date=\"%s\"" %(self.get_redis_tick_table(cdate), cdate)
        return self.mysql_client.get(sql)
    
    def get_k_data_in_range(self, start_date, end_date, dtype = 9):
        table_name = self.data_type_dict[dtype]
        sql = "select * from %s where cdate between \"%s\" and \"%s\"" %(table_name, start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, date = None, dtype = 9):
        table_name = self.data_type_dict[dtype] 
        if date is not None:
            sql = "select * from %s where date=\"%s\"" %(table_name, date)
        else:
            sql = "select * from %s" % table_name
        return self.mysql_client.get(sql)

    def is_after_release(self, code_id, _date):
        time2Market = self.get('timeToMarket')
        t = time.strptime(str(time2Market), "%Y%m%d")
        y,m,d = t[0:3]
        time2Market = datetime(y,m,d)
        return (datetime.strptime(_date, "%Y-%m-%d") - time2Market).days > 0

if __name__ == "__main__":
    #cs = CStock('601606')
    cs = CStock('601318')
    bonus_info = pd.read_csv("/data/tdx/base/bonus.csv", sep = ',', dtype = {'code' : str, 'market': int, 'type': int, 'money': float, 'price': float, 'count': float, 'rate': float, 'date': int})
    cs.set_k_data(bonus_info, '2018-09-28')
