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
from unittest.mock import patch

class CMySQLTest(unittest.TestCase):
    def setUp(self):
        print('In setUp()')

    def tearDown(self):
        print('In tearDown()')

    @patch('cmysql.pd.read_sql_query')
    def test_get_all_tables(self, mock_read_sql_query):
        res = [1., 2., 3.]
        mock_read_sql_query.return_value = pd.DataFrame({'Tables_in_stock': Series(res)})
        test_cm = cmysql.CMySQL(ct.DB_INFO)
        self.assertTrue(test_cm.get_all_tables() == res)

        mock_read_sql_query.return_value = pd.DataFrame({'Tables_in_stock': Series()})
        test_cm = cmysql.CMySQL(ct.DB_INFO)
        self.assertTrue(len(test_cm.get_all_tables()) == 0)

    @patch('cmysql.pd.read_sql_query')
    @patch('cmysql.pd.DataFrame.to_sql')
    def test_set_get(self, mock_to_sql, mock_read_sql_query):
        res = [1., 2., 3.]
        data = pd.DataFrame({'Tables_in_stock': Series(res)})
        mock_to_sql.return_value = None
        mock_read_sql_query.return_value = data
        test_cm = cmysql.CMySQL(ct.DB_INFO)
        data1 = test_cm.get("sql")
        self.assertTrue(test_cm.set(data, "table") == None)
        self.assertTrue(test_cm.get("sql").equals(data))

if __name__ == '__main__':
    unittest.main()
