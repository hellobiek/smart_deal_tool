# coding=utf-8
import sys
import unittest
from unittest import mock
from os import path
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
from unittest.mock import patch

import common

import datetime
from datetime import datetime,timedelta

class CommonTest(unittest.TestCase):
    def setUp(self):
        print('In setUp()')

    def tearDown(self):
        print('In tearDown()')

    def test_delta_days(self):
        _from = "2019-06-07"
        _to = "2019-06-07"
        self.assertEqual(common.delta_days(_from, _to), 0)

    def test_is_collecting_time(self):
        dtime = datetime(2018, 5, 19, 23, 46, 52, 957084)
        self.assertTrue(not common.is_collecting_time(dtime))
        dtime = datetime(2018, 5, 18, 19, 30, 00, 0)
        self.assertTrue(common.is_collecting_time(dtime))

    def test_is_trading_time(self):
        dtime = datetime(2018, 5, 19, 10, 46, 52, 957084)
        self.assertTrue(common.is_trading_time(dtime))
        dtime = datetime(2018, 5, 18, 19, 30, 00, 0)
        self.assertTrue(not common.is_trading_time(dtime))

if __name__ == '__main__':
    unittest.main()
