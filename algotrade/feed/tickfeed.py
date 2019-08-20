# -*- coding: utf-8 -*-
import abc
from pyalgotrade import feed
from pyalgotrade import dispatchprio
from algotrade.feed import bar, tickds
class BaseBarFeed(feed.BaseFeed):
    def __init__(self, frequency, maxLen=None):
        super(BaseBarFeed, self).__init__(maxLen)
        self.__frequency = frequency
        self.__useAdjustedValues = False
        self.__defaultInstrument = None
        self.__currentBars = None
        self.__lastBars = {}

    def reset(self):
        self.__currentBars = None
        self.__lastBars = {}
        super(BaseBarFeed, self).reset()

    def setUseAdjustedValues(self, useAdjusted):
        if useAdjusted and not self.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        # This is to affect future dataseries when they get created.
        self.__useAdjustedValues = useAdjusted
        # Update existing dataseries
        for instrument in self.getRegisteredInstruments():
            self[instrument].setUseAdjustedValues(useAdjusted)

    # Return the datetime for the current bars.
    @abc.abstractmethod
    def getCurrentDateTime(self):
        raise NotImplementedError()

    # Return True if bars provided have adjusted close values.
    @abc.abstractmethod
    def barsHaveAdjClose(self):
        raise NotImplementedError()

    # Subclasses should implement this and return a pyalgotrade.bar.Bars or None if there are no bars.
    @abc.abstractmethod
    def getNextBars(self):
        raise NotImplementedError()

    def createDataSeries(self, key, maxLen):
        ret = tickds.TickDataSeries(maxLen)
        ret.setUseAdjustedValues(self.__useAdjustedValues)
        return ret

    def getNextValues(self):
        dateTime = None
        bars = self.getNextBars()
        if bars is not None:
            dateTime = bars.getDateTime()
            #if self.__currentBars is not None: print(dateTime, self.__currentBars.getDateTime())
            # Check that current bar datetimes are greater than the previous one.
            if self.__currentBars is not None and self.__currentBars.getDateTime() >= dateTime:
                raise Exception(
                    "Bar date times are not in order. Previous datetime was %s and current datetime is %s" % (
                        self.__currentBars.getDateTime(),
                        dateTime
                    )
                )
            # Update self.__currentBars and self.__lastBars
            self.__currentBars = bars
            for instrument in bars.getInstruments():
                self.__lastBars[instrument] = bars[instrument]
        return (dateTime, bars)

    def getFrequency(self):
        return self.__frequency

    def isIntraday(self):
        return self.__frequency < bar.Frequency.DAY

    def getCurrentBars(self):
        """Returns the current :class:`pyalgotrade.bar.Bars`."""
        return self.__currentBars

    def getLastBar(self, instrument):
        """Returns the last :class:`pyalgotrade.bar.Bar` for a given instrument, or None."""
        return self.__lastBars.get(instrument, None)

    def getDefaultInstrument(self):
        """Returns the last instrument registered."""
        return self.__defaultInstrument

    def getRegisteredInstruments(self):
        """Returns a list of registered intstrument names."""
        return self.getKeys()

    def registerInstrument(self, instrument):
        self.__defaultInstrument = instrument
        self.registerDataSeries(instrument)

    def getDataSeries(self, instrument=None):
        """Returns the :class:`pyalgotrade.dataseries.bards.BarDataSeries` for a given instrument.

        :param instrument: Instrument identifier. If None, the default instrument is returned.
        :type instrument: string.
        :rtype: :class:`pyalgotrade.dataseries.bards.BarDataSeries`.
        """
        if instrument is None:
            instrument = self.__defaultInstrument
        return self[instrument]

    def getDispatchPriority(self):
        return dispatchprio.BAR_FEED

# This class is used by the optimizer module. The barfeed is already built on the server side,
# and the bars are sent back to workers.
class OptimizerBarFeed(BaseBarFeed):
    def __init__(self, frequency, instruments, bars, maxLen=None):
        super(OptimizerBarFeed, self).__init__(frequency, maxLen)
        for instrument in instruments:
            self.registerInstrument(instrument)
        self.__bars = bars
        self.__nextPos = 0
        self.__currDateTime = None

        try:
            self.__barsHaveAdjClose = self.__bars[0][instruments[0]].getAdjClose() is not None
        except Exception:
            self.__barsHaveAdjClose = False

    def getCurrentDateTime(self):
        return self.__currDateTime

    def barsHaveAdjClose(self):
        return self.__barsHaveAdjClose

    def start(self):
        super(OptimizerBarFeed, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def peekDateTime(self):
        ret = None
        if self.__nextPos < len(self.__bars):
            ret = self.__bars[self.__nextPos].getDateTime()
        return ret

    def getNextBars(self):
        ret = None
        if self.__nextPos < len(self.__bars):
            ret = self.__bars[self.__nextPos]
            self.__currDateTime = ret.getDateTime()
            self.__nextPos += 1
        return ret

    def eof(self):
        return self.__nextPos >= len(self.__bars)
