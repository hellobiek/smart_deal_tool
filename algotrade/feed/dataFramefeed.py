# -*- coding: utf-8 -*-
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import numpy as np
from algotrade.feed import bar
from pyalgotrade.utils import dt
from pyalgotrade.barfeed import common
from algotrade.feed import dataFrameBarfeed
from pyalgotrade.dataseries import DEFAULT_MAX_LEN
from base.cdate import parse_date, parse_date16, parse_date19
######################################################################
# Each bar must be on its own line and fields must be separated by comma (,).
#
# Bars Format:
# Date,Open,High,Low,Close,Volume,Adj Close
#
# The csv Date column must have the following format: YYYY-MM-DD
class RowParser(dataFrameBarfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize

    def __parseDate(self, dateString):
        ret = parse_date(dateString)
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","
    
    def handler(x):
        pass

    def parseBar(self, row):
        if isinstance(row[0], str):
            if len(row[0].strip()) == 19:
                dateTime = parse_date19(row[0])
            elif len(row[0].strip()) == 16:
                dateTime = parse_date16(row[0])
            else:
                dateTime = parse_date(row[0])
        else:
            dateTime =row[0]
       
        open_       = float(row[1]['open'])
        high        = float(row[1]['high'])
        low         = float(row[1]['low'])
        close       = float(row[1]['close'])
        volume      = float(row[1]['volume'])
        adjClose    = float(row[1]['close'])
        if self.__sanitize:
            open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)
        
        key_dict = dict()
        origin_keys = row[1].keys()
        normal_keys = ['open', 'high', 'low', 'close', 'volume', 'code']
        special_keys = list(set(origin_keys).difference(set(normal_keys)))
        for sitem in special_keys:
            value = None if np.isnan(row[1][sitem]) else float(row[1][sitem])
            key_dict[sitem] = value
        return bar.BasicBar(dateTime, open_, high, low, close, volume, adjClose, self.__frequency, extra = key_dict)

class Feed(dataFrameBarfeed.BarFeed):
    """
    A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Yahoo! Finance.
    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK** are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
                   Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.
    note:: When working with multiple instruments:
        * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
        * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """
    def __init__(self, frequency = bar.Frequency.DAY, timezone = None, maxLen = DEFAULT_MAX_LEN):
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if frequency not in [bar.Frequency.DAY, bar.Frequency.WEEK, bar.Frequency.MINUTE]:
            raise Exception("Invalid frequency.")

        super(Feed, self).__init__(frequency, maxLen)
        self.__timezone = timezone
        self.__sanitizeBars = False

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromDataFrame(self, instrument, dataFrame, timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone

        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        dataFrameBarfeed.BarFeed.addBarsFromDataFrame(self, instrument, rowParser, dataFrame)

class TickFeed(dataFrameBarfeed.TickFeed):
    def __init__(self, frequency = bar.Frequency.TRADE, timezone = None, maxLen = DEFAULT_MAX_LEN):
        if isinstance(timezone, int): raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")
        dataFrameBarfeed.TickFeed.__init__(self, frequency, maxLen)
        self.__timezone = timezone
        self.__sanitizeBars = False
        self.__datetime_format = '%Y-%m-%d %H:%M:%S.%f'

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def set_datetime_format(self,datetime_format):
        self.__datetime_format = datetime_format

    def barsHaveAdjClose(self):
        return True

    def addBarsFromDataFrame(self, instrument, dataFrame, timezone=None):
        if isinstance(timezone, int): raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None: timezone = self.__timezone
        dataFrame = dataFrame.sort_values(by='datetime')
        dataFrame.drop_duplicates('datetime', inplace=True)

        read_list = ['open', 'high', 'low', 'close', 'volume', 'amount', 'preclose', 'new_price', 'bought_amount', 'sold_amount', 'bought_volume', 'sold_volume', 'frequency']
        for col in read_list:
            if col not in dataFrame.columns:
                dataFrame[col] = 0

        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        dataFrameBarfeed.TickFeed.addBarsFromDataFrame(self, instrument,rowParser,dataFrame)
