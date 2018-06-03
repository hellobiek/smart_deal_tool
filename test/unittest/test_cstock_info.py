#encoding=utf-8
import sys
import unittest
from unittest import mock
from unittest.mock import patch
from os import path
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
import cmysql
import const as ct
import pandas as pd
import cstock_info as stock_info
from pandas import DataFrame, Series
from common import _fprint

class CStockInfoTest(unittest.TestCase):
    def setUp(self):
        print("Enter:%s" % self._testMethodName)

    def tearDown(self):
        print("Leave:%s" % self._testMethodName)

    @patch('sqlalchemy.create_engine')
    @patch('cstock_info.CStockInfo.create')
    def test_create(self, mock_stock_info_create, mock_create_engine):
        mock_create_engine.return_value = True
        mock_stock_info_create.return_value = False
        with self.assertRaises(Exception):
            stock_info.CStockInfo(ct.DB_INFO, "btable")

        mock_create_engine.return_value = True
        mock_stock_info_create.return_value = True
        stock_info.CStockInfo(ct.DB_INFO, "btable")
        mock_stock_info_create.assert_called()

    @patch('cmysql.CMySQL.create')
    @patch('cmysql.CMySQL.get_all_tables')
    @patch('sqlalchemy.create_engine')
    def test_create(self, mock_create_engine, mock_client_create, mock_client_get_all_tables):
        mock_create_engine.return_value = True
        mock_client_create.return_value = True
        mock_client_get_all_tables.return_value = []
        with self.assertRaises(Exception):
            stock_info.CStockInfo(ct.DB_INFO, "ctable")

        mock_create_engine.return_value = True
        mock_client_create.return_value = True
        mock_client_get_all_tables.return_value = []
        with self.assertRaises(Exception):
            stock_info.CStockInfo(ct.DB_INFO, "ctable")

    @patch('cmysql.CMySQL.set')
    @patch('tushare.get_stock_basics')
    @patch('cmysql.CMySQL.get')
    @patch('cmysql.CMySQL.get_all_tables')
    @patch('cmysql.CMySQL.create')
    @patch('sqlalchemy.create_engine')
    def test_get(self, mock_create_engine, mock_mysql_client_create, mock_mysql_client_get_all_tables, mock_client_get, mock_tushare_get_stock_basics,mock_client_set):
        df = DataFrame({'code': Series(['123456'])})
        mock_create_engine.return_value = True
        mock_mysql_client_create.return_value = False
        mock_mysql_client_get_all_tables.return_value = ["ctable"]
        mock_client_get.return_value = DataFrame({'code': Series(['123456']), 'limitUpNum': Series([0]), 'limitDownNum': Series([0])}) 
        mock_tushare_get_stock_basics.return_value = df  
        xinfo = stock_info.CStockInfo(ct.DB_INFO, "ctable")
        xinfo.init()
        mock_client_set.assert_called_once()

if __name__ == '__main__':
    unittest.main()
