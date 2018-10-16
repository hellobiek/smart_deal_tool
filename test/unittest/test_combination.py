#encoding=utf-8
import sys
import unittest
from unittest import mock
from os import path
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
import cmysql
import const as ct
import pandas as pd
from pandas import Series
import combination
import combination_info
from unittest.mock import patch

class CombinationTest(unittest.TestCase):
    def setUp(self):
        print("Enter:%s" % self.__class__.__name__)

    def tearDown(self):
        print("Leave:%s" % self.__class__.__name__)

    @patch('cmysql.CMySQL.create')
    @patch('cmysql.CMySQL.get_all_tables')
    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    @patch('combination.Combination.get')
    @patch('combination.Combination.create')
    def test_create_static(self,mock_create,mock_combination_get,mock_combination_info_create,mock_sqlalchemy_create_engine, mock_get_all_table, mock_mysql_create):
        mock_sqlalchemy_create_engine.return_value = True
        mock_combination_info_create.return_value = True
        mock_combination_get.return_value = True
        mock_create.return_value = True
        mock_mysql_create.return_value = True
        mock_get_all_table.return_value = ["table"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(cm.create_static())
        mock_get_all_table.return_value = ["table1"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(cm.create_static())
        mock_mysql_create.return_value = False
        mock_get_all_table.return_value = ["table1"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(not cm.create_static())

    @patch('cmysql.CMySQL.create')
    @patch('cmysql.CMySQL.get_all_tables')
    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    @patch('combination.Combination.get')
    @patch('combination.Combination.create')
    def test_create_realtime(self,mock_create,mock_combination_get,mock_combination_info_create,mock_sqlalchemy_create_engine, mock_get_all_table, mock_mysql_create):
        mock_sqlalchemy_create_engine.return_value = True
        mock_combination_info_create.return_value = True
        mock_combination_get.return_value = True
        mock_create.return_value = True
        mock_mysql_create.return_value = True
        mock_get_all_table.return_value = ["table1"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(cm.create_realtime())

        mock_mysql_create.return_value = False
        mock_get_all_table.return_value = ["table1"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(not cm.create_realtime())

        mock_mysql_create.return_value = False
        mock_get_all_table.return_value = ["c111111_realtime"]
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        self.assertTrue(cm.create_realtime())

    @patch('combination.Combination.create_static')
    @patch('combination.Combination.create_realtime')
    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    @patch('combination.Combination.get')
    def test_create(self,mock_combination_get,mock_combination_info_create,mock_sqlalchemy_create_engine,mock_create_realtime,mock_create_static):
        mock_sqlalchemy_create_engine.return_value = True
        mock_combination_info_create.return_value = True
        mock_combination_get.return_value = True

        cm = combination.Combination("111111", ct.DB_INFO, "table")
        mock_create_static.return_value = True
        mock_create_realtime.return_value = True
        self.assertTrue(cm.create())

        mock_create_static.return_value = False
        mock_create_realtime.return_value = True
        with self.assertRaises(Exception):
            combination.Combination("111111", ct.DB_INFO, "table")

    @patch('cmysql.CMySQL.set')
    @patch('combination.ts.get_k_data')
    @patch('combination.Combination.get_k_data')
    @patch('sqlalchemy.create_engine')
    @patch('combination_info.CombinationInfo.create')
    @patch('combination.Combination.get')
    def test_create(self,mock_combination_get,mock_combination_info_create,mock_sqlalchemy_create_engine,mock_com_get_k_data,ts_get_k_data,cs_mysql_set):
        mock_sqlalchemy_create_engine.return_value = True
        mock_combination_info_create.return_value = True
        mock_combination_get.return_value = True
        cm = combination.Combination("111111", ct.DB_INFO, "table")
        mock_com_get_k_data.return_value = pd.DataFrame({'date': Series(['20170921'])})
        ts_get_k_data.return_value = pd.DataFrame({'date': Series(['20170923'])})
        cm.init()
        cs_mysql_set.assert_called()

    @patch('cmysql.CMySQL.get')
    @patch('combination.Combination.create')
    @patch('combination_info.CombinationInfo.create')
    def test_get_k_data(self, mock_create_info, mock_create, mock_sql_get):
        mock_create.return_value = True
        mock_create_info.return_value = True
        cs = combination.Combination('111111', ct.DB_INFO, 'testa')
        cs.get_k_data('2017-3-18')
        mock_sql_get.assert_called_with("select * from c111111_D where date=\"2017-3-18\"")

        cs = combination.Combination('111111', ct.DB_INFO, 'testa')
        cs.get_k_data()
        mock_sql_get.assert_called_with("select * from c111111_D")

if __name__ == '__main__':
    unittest.main()
