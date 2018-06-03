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
from pandas import DataFrame, Series
from common import _fprint
import combination_info as cm_info

class CombinationInfoTest(unittest.TestCase):
    def setUp(self):
        print("Enter:%s" % self._testMethodName)

    def tearDown(self):
        print("Leave:%s" % self._testMethodName)

    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    def test_init(self, mock_combination_info_create, mock_create_engine):
        mock_create_engine.return_value = True
        mock_combination_info_create.return_value = False
        with self.assertRaises(Exception):
            cm_info.CombinationInfo(ct.DB_INFO, "table")

    @patch('sqlalchemy.create_engine')
    @patch('cmysql.CMySQL.create')
    @patch('cmysql.CMySQL.get_all_tables')
    def test_create(self, mock_combination_get_all_tables, mock_mysql_client_create, mock_create_engine):
        mock_create_engine.return_value = True
        mock_mysql_client_create.return_value = False
        mock_combination_get_all_tables.return_value = []
        with self.assertRaises(Exception):
            cm_info.CombinationInfo(ct.DB_INFO, "xtable")

        mock_mysql_client_create.return_value = True
        self.assertTrue(cm_info.CombinationInfo(ct.DB_INFO, "xtable"))

    @patch('cmysql.CMySQL.get')
    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    def test_init(self, mock_combination_info_create, mock_create_engine, mock_mysql_get):
        df = DataFrame({'name':[1,2,3]})
        mock_mysql_get.return_value = df
        mock_create_engine.return_value = True
        mock_combination_info_create.return_value = True
        xinfo = cm_info.CombinationInfo(ct.DB_INFO, "ztable")
        zlist = xinfo.get_index_list("x")
        self.assertEqual(zlist, [1,2,3])

if __name__ == '__main__':
    unittest.main()
