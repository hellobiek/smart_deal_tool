#coding=utf-8
from pyalgotrade import dataseries
class TickDataSeries(dataseries.SequenceDataSeries):
    def __init__(self, maxLen=None):
        super(TickDataSeries, self).__init__(maxLen)
        self.__openDS = dataseries.SequenceDataSeries(maxLen)
        self.__closeDS = dataseries.SequenceDataSeries(maxLen)
        self.__highDS = dataseries.SequenceDataSeries(maxLen)
        self.__lowDS = dataseries.SequenceDataSeries(maxLen)
        self.__volumeDS = dataseries.SequenceDataSeries(maxLen)
        self.__adjCloseDS = dataseries.SequenceDataSeries(maxLen)
        self.__apDS = dataseries.SequenceDataSeries(maxLen)
        self.__bpDS = dataseries.SequenceDataSeries(maxLen)
        self.__avDS = dataseries.SequenceDataSeries(maxLen)
        self.__bvDS = dataseries.SequenceDataSeries(maxLen)
        self.__precloseDS = dataseries.SequenceDataSeries(maxLen)
        self.__extraDS = {}
        self.__useAdjustedValues = False

    def __getOrCreateExtraDS(self, name):
        ret = self.__extraDS.get(name)
        if ret is None:
            ret = dataseries.SequenceDataSeries(self.getMaxLen())
            self.__extraDS[name] = ret
        return ret

    def setUseAdjustedValues(self, useAdjusted):
        self.__useAdjustedValues = useAdjusted

    def append(self, bar):
        self.appendWithDateTime(bar.getDateTime(), bar)

    def appendWithDateTime(self, dateTime, bar):
        assert (dateTime is not None)
        assert (bar is not None)
        bar.setUseAdjustedValue(self.__useAdjustedValues)

        super(TickDataSeries, self).appendWithDateTime(dateTime, bar)

        self.__openDS.appendWithDateTime(dateTime, bar.getOpen())
        self.__closeDS.appendWithDateTime(dateTime, bar.getClose())
        self.__highDS.appendWithDateTime(dateTime, bar.getHigh())
        self.__lowDS.appendWithDateTime(dateTime, bar.getLow())
        self.__volumeDS.appendWithDateTime(dateTime, bar.getVolume())
        self.__adjCloseDS.appendWithDateTime(dateTime, bar.getAdjClose())

        self.__apDS.appendWithDateTime(dateTime, bar.getAp())
        self.__bpDS.appendWithDateTime(dateTime, bar.getBp())
        self.__avDS.appendWithDateTime(dateTime, bar.getAv())
        self.__bvDS.appendWithDateTime(dateTime, bar.getBv())
        self.__precloseDS.appendWithDateTime(dateTime, bar.getPreclose())
        # Process extra columns.
        for name, value in bar.getExtraColumns().items():
            extraDS = self.__getOrCreateExtraDS(name)
            extraDS.appendWithDateTime(dateTime, value)

    def getApDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the open prices."""
        return self.__apDS

    def getBpDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the close prices."""
        return self.__bpDS

    def getAvDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the high prices."""
        return self.__avDS

    def getBvDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the low prices."""
        return self.__bvDS

    def getPrecloseDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the volume."""
        return self.__precloseDS

    def getOpenDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the open prices."""
        return self.__openDS

    def getCloseDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the close prices."""
        return self.__closeDS

    def getHighDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the high prices."""
        return self.__highDS

    def getLowDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the low prices."""
        return self.__lowDS

    def getVolumeDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the volume."""
        return self.__volumeDS

    def getAdjCloseDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the adjusted close prices."""
        return self.__adjCloseDS

    def getPriceDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the close or adjusted close prices."""
        if self.__useAdjustedValues:
            return self.__adjCloseDS
        else:
            return self.__closeDS

    def getExtraDataSeries(self, name):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` for an extra column."""
        return self.__getOrCreateExtraDS(name)
