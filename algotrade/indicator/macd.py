from pyalgotrade.technical.macd import MACD
class DMACD(MACD):
    def __init__(self, dataSeries, fastEMA, slowEMA, signalEMA, maxLen=None):
        super(DMACD, self).__init__(dataSeries, fastEMA, slowEMA, signalEMA, maxLen)

    def 
