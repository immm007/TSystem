class Frequency:
    TRADE = -1
    SECOND = 1
    MINUTE = 60
    FIVE_MINUTE = 5*60
    FIFTEEN_MINUTE = 15*60
    THIRTY_MINUTE = 30*60
    HOUR = 60*60
    DAY = 24*60*60
    WEEK = 24*60*60*7
    MONTH = 24*60*60*31


class Bar:
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__frequency',
    )

    def __init__(self, dateTime, open_, high, low, close, volume, frequency):
        if high < low:
            raise Exception("high < low on %s" % (dateTime))
        elif high < open_:
            raise Exception("high < open on %s" % (dateTime))
        elif high < close:
            raise Exception("high < close on %s" % (dateTime))
        elif low > open_:
            raise Exception("low > open on %s" % (dateTime))
        elif low > close:
            raise Exception("low > close on %s" % (dateTime))

        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__frequency = frequency

    def __setstate__(self, state):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__frequency
         )

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__frequency
        )

    def __eq__(self, other):
        return (self.dateTime==other.dateTime and self.frequency == other.frequency and self.close==other.close
        and self.high==other.high and self.open==other.open and self.low==other.low and self.volume==other.volume)

    def __str__(self):
        return "datetime: %s open: %s close: %s high: %s low: %s volume %s frequency: %s" %\
               ( self.dateTime,self.open,self.close,self.high,self.low,self.volume,self.frequency)

    @property
    def dateTime(self):
        return self.__dateTime

    @property
    def open(self):
        return self.__open

    @property
    def high(self):
        return self.__high

    @property
    def low(self):
        return self.__low

    @property
    def close(self):
        return self.__close

    @property
    def volume(self):
        return self.__volume

    @property
    def frequency(self):
        return self.__frequency

