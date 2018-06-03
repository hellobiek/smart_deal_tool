#encoding=utf-8
import sys
import unittest
from unittest import mock
from os import path
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
import const as ct
import pandas as pd
from pandas import Series
from unittest.mock import patch
from cstock import CStock
from common import _fprint

class CStockTest(unittest.TestCase):
    def setUp(self):
        print('In setUp()')

    def tearDown(self):
        print('In tearDown()')

    @patch('cstock_info.CStockInfo.create')
    @patch('cstock.CStock.get')
    @patch('cstock.CStock.create')
    def test_is_subnew(self, mock_create, mock_get, mock_createInfo):
        mock_createInfo.return_value = True
        mock_create.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        self.assertEqual(cs.is_subnew(time2Market = '0'), False)

        mock_createInfo.return_value = True
        mock_create.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get.return_value = '20131221'
        self.assertEqual(cs.is_subnew(), False)

        mock_createInfo.return_value = True
        mock_create.return_value = True
        mock_get.return_value = '20171221'
        cs = CStock(ct.DB_INFO, '111111', 'test')
        self.assertEqual(cs.is_subnew(time2Market = None), True)

    @patch('cstock.CMySQL.create')
    @patch('cstock_info.CStockInfo.create')
    @patch('cstock.CMySQL.get_all_tables')
    @patch('cstock.CStock.create')
    def test_create_static(self, mock_create, mock_get_all_tables, mock_createInfo, mock_mysql_create):
        mock_create.return_value = True
        mock_createInfo.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = ['111111_D']
        self.assertEqual(cs.create_static(), True)

        mock_create.return_value = True
        mock_createInfo.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = []
        mock_mysql_create.return_value = True 
        self.assertEqual(cs.create_static(), True)

        mock_create.return_value = True
        mock_createInfo.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = []
        mock_mysql_create.return_value = False
        self.assertEqual(cs.create_static(), False)

    @patch('cstock.CMySQL.create')
    @patch('cstock.CMySQL.get_all_tables')
    @patch('cstock_info.CStockInfo.create')
    @patch('cstock.CStock.create')
    def test_create_realtime(self,mock_create, mock_create_info, mock_get_all_tables, mock_mysql_create):
        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = []
        mock_mysql_create.return_value = False
        self.assertEqual(cs.create_realtime(), False)

        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = []
        mock_mysql_create.return_value = True
        self.assertEqual(cs.create_realtime(), True)

        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'test')
        mock_get_all_tables.return_value = [cs.realtime_table]
        mock_mysql_create.return_value = False
        self.assertEqual(cs.create_realtime(), True)

    @patch('cstock.CMySQL.get')
    @patch('cstock_info.CStockInfo.create')
    @patch('cstock.CStock.create')
    def test_get(self,mock_create, mock_create_info, mock_mysql_get):
        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'testa')
        mock_mysql_get.return_value = pd.DataFrame({'timeToMarket': Series(['20170923'])})
        self.assertEqual(cs.get('timeToMarket'), '20170923')

    @patch('cstock.CMySQL.set')
    @patch('cstock.ts.get_k_data')
    @patch('cstock.CStock.get_k_data')
    @patch('cstock_info.CStockInfo.create')
    @patch('cstock.CStock.create')
    def test_init(self, mock_create, mock_create_info, mock_get_k_data, mock_ts_get_data, mock_mysql_set):
        mock_create.return_value = True
        mock_create_info.return_value = True
        mock_get_k_data.return_value = pd.DataFrame({'date': Series(['20170922'])}) 
        mock_ts_get_data.return_value = pd.DataFrame({'date': Series(['20170923'])}) 
        _data = pd.DataFrame({'date': Series(['20170922','20170923'])}).reset_index(drop = True)
        cs = CStock(ct.DB_INFO, '111111', 'testb')
        cs.init()
        mock_mysql_set.assert_called_once()

    #@patch('common.is_trading_time')
    #@patch('cstock.CMySQL.set')
    #@patch('cstock_info.CStockInfo.get')
    #@patch('cstock.ts.get_realtime_quotes')
    #@patch('cstock_info.CStockInfo.create')
    #@patch('cstock.CStock.create')
    #def test_run(self, mock_create, mock_create_info, mock_ts_get_realtime_quotes, mock_info_get, mock_sql_set, mock_is_trading_time):
    #    mock_info_get.return_value = 10
    #    mock_create.return_value = True
    #    mock_create_info.return_value = True
    #    mock_ts_get_realtime_quotes.return_value = pd.DataFrame({'code': Series(['111111']), 'volume': Series([123456]), 'price': Series([12]), 'pre_close': Series([10]), 'p_change': Series([9])}) 
    #    mock_sql_set.return_value = True
    #    mock_is_trading_time.return_value = False
    #    cs = CStock(ct.DB_INFO, '111111', 'testa')
    #    cs.run(1)
    #    mock_sql_set.assert_called_once()
    #    mock_info_get.assert_called_once()
    #    mock_is_trading_time.assert_called_once()
    #    mock_ts_get_realtime_quotes.assert_called_once()

    @patch('cstock.CMySQL.get')
    @patch('cstock.CStock.create')
    @patch('cstock_info.CStockInfo.create')
    def test_get_k_data(self, mock_create_info, mock_create, mock_sql_get):
        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = CStock(ct.DB_INFO, '111111', 'testa')
        cs.get_k_data('2017-3-18')
        mock_sql_get.assert_called_with("select * from `111111` where date=\"2017-3-18\"")

        cs = CStock(ct.DB_INFO, '111111', 'testa')
        cs.get_k_data()
        mock_sql_get.assert_called_with("select * from `111111`")

        cs = CStock(ct.DB_INFO, '111111a', 'testa')
        cs.get_k_data()
        mock_sql_get.assert_called_with("select * from 111111a")

        cs = CStock(ct.DB_INFO, '111111a', 'testa')
        cs.get_k_data('2017-3-19')
        mock_sql_get.assert_called_with("select * from 111111a where date=\"2017-3-19\"")

if __name__ == '__main__':
    unittest.main()
