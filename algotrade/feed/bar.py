# -*- coding: utf-8 -*-
import abc
class Frequency(object):
    """
    Enum like class for bar frequencies. Valid values are:
    * **Frequency.TRADE**: The bar represents a single trade.
    * **Frequency.SECOND**: The bar summarizes the trading activity during 1 second.
    * **Frequency.MINUTE**: The bar summarizes the trading activity during 1 minute.
    * **Frequency.HOUR**: The bar summarizes the trading activity during 1 hour.
    * **Frequency.DAY**: The bar summarizes the trading activity during 1 day.
    * **Frequency.WEEK**: The bar summarizes the trading activity during 1 week.
    * **Frequency.MONTH**: The bar summarizes the trading activity during 1 month.
    """
    # It is important for frequency values to get bigger for bigger windows.
    TRADE = -1
    SECOND = 1
    MINUTE = 60
    HOUR = 60 * 60
    DAY = 24 * 60 * 60
    WEEK = 24 * 60 * 60 * 7
    MONTH = 24 * 60 * 60 * 31

class Bar(object):
    """
        A Bar is a summary of the trading activity for a security in a given period.
        note::This is a base class and should not be used directly.
    """
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def setUseAdjustedValue(self, useAdjusted):
        raise NotImplementedError()

    @abc.abstractmethod
    def getUseAdjValue(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def getDateTime(self):
        """Returns the :class:`datetime.datetime`."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getOpen(self, adjusted=False):
        """Returns the opening price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getHigh(self, adjusted=False):
        """Returns the highest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getLow(self, adjusted=False):
        """Returns the lowest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getClose(self, adjusted=False):
        """Returns the closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getVolume(self):
        """Returns the volume."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getAdjClose(self):
        """Returns the adjusted closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getFrequency(self):
        """The bar's period."""
        raise NotImplementedError()

    def getTypicalPrice(self):
        """Returns the typical price."""
        return (self.getHigh() + self.getLow() + self.getClose()) / 3.0

    @abc.abstractmethod
    def getPrice(self):
        """Returns the closing or adjusted closing price."""
        raise NotImplementedError()

    def getExtraColumns(self):
        return {}

class BasicBar(Bar):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__high',
        '__low',
        '__close',
        '__volume',
        '__adjClose',
        '__frequency',
        '__useAdjustedValue',
        '__extra',
    )
    def __init__(self, dateTime, open_, high, low, close, volume, adjClose, frequency, extra={}):
        if high < low: raise Exception("high < low on %s" % (dateTime))
        elif high < open_: raise Exception("high < open on %s" % (dateTime))
        elif high < close: raise Exception("high < close on %s" % (dateTime))
        elif low > open_: raise Exception("low > open on %s" % (dateTime))
        elif low > close: raise Exception("low > close on %s" % (dateTime))

        self.__dateTime = dateTime
        self.__open = open_
        self.__high = high
        self.__low = low
        self.__close = close
        self.__volume = volume
        self.__adjClose = adjClose
        self.__frequency = frequency
        self.__useAdjustedValue = False
        self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
         self.__open,
         self.__high,
         self.__low,
         self.__close,
         self.__volume,
         self.__adjClose,
         self.__frequency,
         self.__useAdjustedValue,
         self.__extra) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__high,
            self.__low,
            self.__close,
            self.__volume,
            self.__adjClose,
            self.__frequency,
            self.__useAdjustedValue,
            self.__extra
        )

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted: raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, useAdjusted = False):
        return self.__open

    def getAdjClose(self, useAdjusted = False):
        return self.__adjClose

    def getHigh(self, useAdjusted = False):
        return self.__high

    def getLow(self, useAdjusted = False):
        return self.__low

    def getClose(self, useAdjusted = False):
        return self.__close

    def getVolume(self):
        return self.__volume

    def getFrequency(self):
        return self.__frequency

    def getPrice(self):
        return self.__close

    def getExtraColumns(self):
        return self.__extra

class Bars(object):
    """
    A group of :class:`Bar` objects.
    :param barDict: A map of instrument to :class:`Bar` objects.
    :type barDict: map.
    :note::All bars must have the same datetime.
    """
    def __init__(self, barDict):
        if len(barDict) == 0: raise Exception("No bars supplied")
        # Check that bar datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentBar in barDict.items():
            if firstDateTime is None:
                firstDateTime = currentBar.getDateTime()
                firstInstrument = instrument
            elif currentBar.getDateTime() != firstDateTime:
                raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentBar.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))
        self.__barDict = barDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return self.__barDict.items()

    def keys(self):
        return self.__barDict.keys()

    def getInstruments(self):
        """Returns the instrument symbols."""
        return self.__barDict.keys()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)

class BasicTick(object):
    __slots__ = (
        '__dateTime',
        '__open',
        '__high',
        '__low',
        '__close',
        '__preclose',
        '__volume',
        '__amount',
        '__bp1',
        '__bv1',
        '__bp2',
        '__bv2',
        '__bp3',
        '__bv3',
        '__bp4',
        '__bv4',
        '__bp5',
        '__bv5',
        '__ap1',
        '__av1',
        '__ap2',
        '__av2',
        '__ap3',
        '__av3',
        '__ap4',
        '__av4',
        '__ap5',
        '__av5',
        '__frequency',
        '__extra',
        '__adjClose',
        '__useAdjustedValue',
    )

    def __init__(self, dateTime, open_, high, low, close, preclose, volume, amount, 
                bp1, bv1, bp2, bv2, bp3, bv3, bp4, bv4, bp5, bv5,  
                ap1, av1, ap2, av2, ap3, av3, ap4, av4, ap5, av5, 
                frequency, extra={}):
        self.__dateTime = dateTime
        self.__open = open_
        self.__high = high
        self.__low = low
        self.__close = close
        self.__preclose = preclose
        self.__volume = volume
        self.__amount = amount
        self.__bp1 = bp1
        self.__bv1 = bv1
        self.__bp2 = bp2
        self.__bv2 = bv2
        self.__bp3 = bp3
        self.__bv3 = bv3
        self.__bp4 = bp4
        self.__bv4 = bv4
        self.__bp5 = bp5
        self.__bv5 = bv5
        self.__ap1 = ap1
        self.__av1 = av1
        self.__ap2 = ap2
        self.__av2 = av2
        self.__ap3 = ap3
        self.__av3 = av3
        self.__ap4 = ap4
        self.__av4 = av4
        self.__ap5 = ap5
        self.__av5 = av5
        self.__frequency = frequency
        self.__extra = extra
        self.__useAdjustedValue = False
        self.__adjClose = close

    def __setstate__(self, state):
        (self.__dateTime,
         self.__open,
         self.__high,
         self.__low,
         self.__close,
         self.__preclose,
         self.__volume,
         self.__amount,
         self.__bp1,
         self.__bv1,
         self.__bp2,
         self.__bv2,
         self.__bp3,
         self.__bv4,
         self.__bp4,
         self.__bv5,
         self.__bp5,
         self.__ap1,
         self.__av1,
         self.__ap2,
         self.__av2,
         self.__ap3,
         self.__av3,
         self.__ap4,
         self.__av4,
         self.__ap5,
         self.__av5,
         self.__frequency,
         self.__adjClose,
         self.__extra) = state

    def __getstate__(self):
        return (self.__dateTime,
                self.__open,
                self.__high,
                self.__low,
                self.__close,
                self.__preclose,
                self.__volume,
                self.__amount,
                self.__bp1,
                self.__bv1,
                self.__bp2,
                self.__bv2,
                self.__bp3,
                self.__bv3,
                self.__bp4,
                self.__bv4,
                self.__bp5,
                self.__bv5,
                self.__ap1,
                self.__av1,
                self.__ap2,
                self.__av2,
                self.__ap3,
                self.__av3,
                self.__ap4,
                self.__av4,
                self.__ap5,
                self.__av5,
                self.__frequency,
                self.__adjClose,
                self.__extra)

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted = False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__open / float(self.__close)
        else:
            return self.__open

    def getHigh(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__high / float(self.__close)
        else:
            return self.__high

    def getLow(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__low / float(self.__close)
        else:
            return self.__low

    def getClose(self, adjusted=False):
        return self.__close

    def getVolume(self):
        return self.__volume

    def getAmount(self):
        return self.__amount

    def getFrequency(self):
        return self.__frequency

    def getBp(self):
        return self.__bp1

    def getBv(self):
        return self.__bv1

    def getAp(self):
        return self.__ap1

    def getAv(self):
        return self.__av1

    def getPreclose(self):
        return self.__preclose

    def getExtraColumns(self):
        return self.__extra

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted and self.__adjClose is None:
            raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getAdjClose(self):
        return self.__close

    def getPrice(self):
        return self.__close

class Ticks(object):
    def __init__(self, barDict):
        if len(barDict) == 0: raise Exception("No bars supplied")
        # Check that bar datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentBar in barDict.items():
            if firstDateTime is None:
                firstDateTime = currentBar.getDateTime()
                firstInstrument = instrument
            elif currentBar.getDateTime() != firstDateTime:
                raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentBar.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))
        self.__barDict = barDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return self.__barDict.items()

    def keys(self):
        return self.__barDict.keys()

    def getInstruments(self):
        """Returns the instrument symbols."""
        return self.__barDict.keys()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)
