#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import re
import time
import json
import requests
import const as ct
import pandas as pd
from cmysql import CMySQL
from log import getLogger
from datetime import datetime
from ccalendar import CCalendar
from common import create_redis_obj, get_day_nday_ago, get_dates_array, smart_get, int_random, loads_jsonp, float_random
class StockExchange(object):
    def __init__(self, market = ct.SH_MARKET_SYMBOL, dbinfo = ct.DB_INFO, redis_host = None):
        self.market       = market
        self.dbinfo       = dbinfo
        self.balcklist    = ['2006-07-10'] if market == ct.SH_MARKET_SYMBOL else list()
        self.logger       = getLogger(__name__)
        self.dbname       = self.get_dbname(market)
        self.redis        = create_redis_obj() if redis_host is None else create_redis_obj(host = redis_host)
        self.header       = {"Host": "query.sse.com.cn",
                             "Referer": "http://www.sse.com.cn/market/stockdata/overview/day/",
                             "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}
        self.mysql_client = CMySQL(self.dbinfo, dbname = self.dbname, iredis = self.redis)
        if not self.mysql_client.create_db(self.dbname):
            raise Exception("create %s failed" % self.dbname)
   
    @staticmethod
    def get_dbname(market):
        return market

    def get_table_name(self):
        return "%s_deal" % self.dbname

    def create_table(self):
        table = self.get_table_name() 
        if table not in self.mysql_client.get_all_tables():
            sql = 'create table if not exists %s(date varchar(10) not null,\
                                                 name varchar(20) not null,\
                                                 amount float,\
                                                 number int,\
                                                 negotiable_value float,\
                                                 market_value float,\
                                                 pe float,\
                                                 totals float,\
                                                 outstanding float,\
                                                 volume float,\
                                                 transactions float,\
                                                 turnover float,\
                                                 PRIMARY KEY (date, name))' % table 
            if not self.mysql_client.create(sql, table): return False
        return True

    def get_k_data_in_range(self, start_date, end_date):
        sql = "select * from %s where date between \"%s\" and \"%s\"" % (self.get_table_name(), start_date, end_date)
        return self.mysql_client.get(sql)

    def get_k_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        sql = "select * from %s where date=\"%s\"" % (self.get_table_name(), cdate)
        return self.mysql_client.get(sql)

    def is_table_exists(self, table_name):
        if self.redis.exists(self.dbname):
            return table_name in set(str(table, encoding = "utf8") for table in self.redis.smembers(self.dbname))
        return False

    def is_date_exists(self, table_name, cdate):
        if self.redis.exists(table_name):
            return cdate in set(str(tdate, encoding = "utf8") for tdate in self.redis.smembers(table_name))
        return False

    def get_url(self):
        if self.market == ct.SH_MARKET_SYMBOL:
            return "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?jsonCallBack=jsonpCallback%s&searchDate=%s&prodType=gp&_=%s"
        else:
            return "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1803&TABKEY=%s&txtQueryDate=%s&random=%s"

    def get_sh_type_name(self, dtype):
        if '1' == dtype:
            return "A股"
        elif '2' == dtype:
            return "B股"
        elif '12' == dtype:
            return "上海市场"
        elif '90' == dtype:
            return "科创板"
        else:
            return None

    def get_data_from_url(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        if self.market == ct.SH_MARKET_SYMBOL:
            current_milli_time = lambda: int(round(time.time() * 1000))
            url = self.get_url() % (int_random(5), cdate, current_milli_time())
            response = smart_get(requests.get, url, headers=self.header)
            if response.status_code != 200:
                self.logger.error("get exchange data failed, response code:%s" % response.status_code)
                return pd.DataFrame()
            json_result = loads_jsonp(response.text)
            if json_result is None:
                self.logger.error("parse exchange data jsonp failed")
                return pd.DataFrame()
            datas = list()
            for json_obj in json_result['result']:
                name = self.get_sh_type_name(json_obj['productType'])
                if name is None:
                    self.logger.error("get unknown type for SH data:%s" % json_obj['productType'])
                    return pd.DataFrame()
                elif name == "科创板":
                    continue
                else:
                    amount           = 0 if json_obj['trdAmt'] == '' else float(json_obj['trdAmt'])
                    number           = 0 if json_obj['istVol'] == '' else int(json_obj['istVol'])
                    negotiable_value = 0 if json_obj['negotiableValue'] == '' else float(json_obj['negotiableValue'])
                    market_value     = 0 if json_obj['marketValue'] == '' else float(json_obj['marketValue'])
                    volume           = 0 if json_obj['trdVol'] == '' else float(json_obj['trdVol'])
                    pe               = 0 if json_obj['profitRate'] == '' else float(json_obj['profitRate'])
                    transactions     = 0 if json_obj['trdTm'] == '' else float(json_obj['trdTm'])
                    turnover         = 0 if json_obj['exchangeRate'] == '' else float(json_obj['exchangeRate'])
                    outstanding      = 0 if turnover == 0 else volume / (100 * turnover)
                    totals           = outstanding
                    data = {'amount': amount,\
                            'number': number,\
                            'negotiable_value': negotiable_value,\
                            'market_value': market_value,\
                            'pe': pe,\
                            'totals': totals,\
                            'outstanding': outstanding,\
                            'volume': volume,\
                            'transactions': transactions,\
                            'turnover': turnover}
                    if any(data.values()):
                        data['name'] = name
                        data['date'] = cdate
                        datas.append(data)
            df = pd.DataFrame.from_dict(datas)
        else:
            datas = list()
            for name, tab in ct.SZ_MARKET_DICT.items():
                url = self.get_url() % (tab, cdate, float_random(17))
                df = smart_get(pd.read_excel, url, usecols = [0, 1])
                if df is None: return pd.DataFrame()
                if df.empty: continue
                if len(df) == 1 and df.values[0][0] == '没有找到符合条件的数据！': continue
                if name == "深圳市场":
                    amount           = 0
                    #amount           = float(df.loc[df['指标名称'] == '市场总成交金额（元）', '本日数值'].values[0].replace(',', '')) / 100000000
                    number           = int(float(df.loc[df['指标名称'] == '上市公司数', '本日数值'].values[0].replace(',', '')))
                    negotiable_value = float(df.loc[df['指标名称'] == '股票流通市值（元）', '本日数值'].values[0].replace(',', '')) / 100000000
                    market_value     = float(df.loc[df['指标名称'] == '股票总市值（元）', '本日数值'].values[0].replace(',', '')) / 100000000
                    pe               = float(df.loc[df['指标名称'] == '股票平均市盈率', '本日数值'].values[0].replace(',', ''))
                    totals           = float(df.loc[df['指标名称'] == '股票总股本（股）', '本日数值'].values[0].replace(',', '')) / 100000000
                    outstanding      = float(df.loc[df['指标名称'] == '股票流通股本（股）', '本日数值'].values[0].replace(',', '')) / 100000000
                    volume           = 0
                    transactions     = 0
                    turnover         = float(df.loc[df['指标名称'] == '股票平均换手率', '本日数值'].values[0])
                else:
                    amount           = float(df.loc[df['指标名称'] == '总成交金额(元)', '本日数值'].values[0].replace(',', '')) / 100000000
                    number           = int(float(df.loc[df['指标名称'] == '上市公司数', '本日数值'].values[0].replace(',', '')))
                    negotiable_value = float(df.loc[df['指标名称'] == '上市公司流通市值(元)', '本日数值'].values[0].replace(',', '')) / 100000000
                    market_value     = float(df.loc[df['指标名称'] == '上市公司市价总值(元)', '本日数值'].values[0].replace(',', '')) / 100000000
                    pe               = float(df.loc[df['指标名称'] == '平均市盈率(倍)', '本日数值'].values[0])
                    totals           = float(df.loc[df['指标名称'] == '总发行股本(股)', '本日数值'].values[0].replace(',', '')) / 100000000
                    outstanding      = float(df.loc[df['指标名称'] == '总流通股本(股)', '本日数值'].values[0].replace(',', '')) / 100000000
                    volume           = float(df.loc[df['指标名称'] == '总成交股数', '本日数值'].values[0].replace(',', '')) / 100000000
                    transactions     = float(df.loc[df['指标名称'] == '总成交笔数', '本日数值'].values[0].replace(',', '')) / 10000
                    turnover         = 100 * volume / outstanding
                data = {
                    'name': name,\
                    'date': cdate,\
                    'amount': amount,\
                    'number': number,\
                    'negotiable_value': negotiable_value,\
                    'market_value': market_value,\
                    'pe': pe,\
                    'totals': totals,\
                    'outstanding': outstanding,\
                    'volume': volume,\
                    'transactions': transactions,\
                    'turnover': turnover
                }
                datas.append(data)
            df = pd.DataFrame.from_dict(datas)
            if not df.empty:
                df.at[df.name == "深圳市场", 'amount']       = df.amount.sum() - df.loc[df.name == "深圳市场", 'amount']
                df.at[df.name == "深圳市场", 'volume']       = df.volume.sum() - df.loc[df.name == "深圳市场", 'volume']
                df.at[df.name == "深圳市场", 'transactions'] = df.transactions.sum() - df.loc[df.name == "深圳市场", 'transactions']
        return df

    def set_k_data(self, cdate = datetime.now().strftime('%Y-%m-%d')):
        table_name = self.get_table_name()
        if not self.is_table_exists(table_name):
            if not self.create_table():
                self.logger.error("create tick table failed")
                return False
            self.redis.sadd(self.dbname, table_name)

        if self.is_date_exists(table_name, cdate): 
            self.logger.debug("existed table:%s, date:%s" % (table_name, cdate))
            return True

        df = self.get_data_from_url(cdate)
        if df.empty:
            self.logger.debug("get data from %s failed, date:%s" % (self.market, cdate))
            return False

        if self.mysql_client.set(df, table_name):
            self.redis.sadd(table_name, cdate)
            return True
        return False

    def update(self, end_date = None, num = 10):
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = get_day_nday_ago(end_date, num = num, dformat = "%Y-%m-%d")
        succeed = True
        for mdate in get_dates_array(start_date, end_date):
            if mdate in self.balcklist: continue
            if CCalendar.is_trading_day(mdate, redis = self.redis):
                if not self.set_k_data(mdate):
                    succeed = False
                    self.logger.info("market %s for %s set failed" % (self.market, mdate))
        return succeed

if __name__ == '__main__':
    #for market in [ct.SH_MARKET_SYMBOL, ct.SZ_MARKET_SYMBOL]:
    for market in [ct.SZ_MARKET_SYMBOL]:
        av   = StockExchange(market)
        data = av.update()
        print(data)
