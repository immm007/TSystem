# PyAlgoTrade
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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import abc

from pyalgotrade import observer
from pyalgotrade import dataseries

def feed_iterator(feed):
    feed.start()
    try:
        while not feed.eof():
            yield feed.getNextValuesAndUpdateDS()
    finally:
        feed.stop()
        feed.join()


class BaseFeed(observer.Subject):
    """Base class for feeds.

    :param maxLen: The maximum number of values that each :class:`pyalgotrade.dataseries.DataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end.
    :type maxLen: int.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, maxLen):
        super(BaseFeed, self).__init__()

        maxLen = dataseries.get_checked_max_len(maxLen)

        self.__ds = {}
        self.__event = observer.Event()
        self.__maxLen = maxLen

    def reset(self):
        keys = list(self.__ds.keys())
        self.__ds = {}
        for key in keys:
            self.registerDataSeries(key)

    # Subclasses should implement this and return the appropriate dataseries for the given key.
    @abc.abstractmethod
    def createDataSeries(self, key, maxLen):
        raise NotImplementedError()

    # Subclasses should implement this and return a tuple with two elements:
    # 1: datetime.datetime.
    # 2: dictionary or dict-like object.
    @abc.abstractmethod
    def getNextValues(self):
        raise NotImplementedError()

    def registerDataSeries(self, key):
        if key not in self.__ds:
            self.__ds[key] = self.createDataSeries(key, self.__maxLen)

    def getNextValuesAndUpdateDS(self):
        dateTime, values = self.getNextValues()
        if dateTime is not None:
            for key, value in list(values.items()):
                # Get or create the datseries for each key.
                try:
                    ds = self.__ds[key]
                except KeyError:
                    ds = self.createDataSeries(key, self.__maxLen)
                    self.__ds[key] = ds
                ds.appendWithDateTime(dateTime, value)
        return (dateTime, values)

    def __iter__(self):
        return feed_iterator(self)

    def getNewValuesEvent(self):
        """Returns the event that will be emitted when new values are available.
        To subscribe you need to pass in a callable object that receives two parameters:

         1. A :class:`datetime.datetime` instance.
         2. The new value.
        """
        return self.__event

    def dispatch(self):
        dateTime, values = self.getNextValuesAndUpdateDS()
        if dateTime is not None:
            self.__event.emit(dateTime, values)
        return dateTime is not None

    def getKeys(self):
        return list(self.__ds.keys())

    def __getitem__(self, key):
        """Returns the :class:`pyalgotrade.dataseries.DataSeries` for a given key."""
        return self.__ds[key]

    def __contains__(self, key):
        """Returns True if a :class:`pyalgotrade.dataseries.DataSeries` for the given key is available."""
        return key in self.__ds


class BaseLiveFeed(observer.Subject):
    def __init__(self):
        super().__init__()
        self.__stop = True
        self.__stocks = {}
        self.__event = observer.Event()

    def start(self):
        self.__stop = False

    def stop(self):
        self.__stop = True

    def eof(self):
        return self.__stop

    def join(self):
        pass

    def peekDateTime(self):
        return None

    def getNewValuesEvent(self):
        return self.__event

    def dispatch(self):
        ret = False
        for code in self.__stocks:
            stock = self.__stocks[code]
            for frequency in stock.analysisPeriods:
                bar,update= self.getNextBar(code,frequency)
                if bar is not None:
                    ret = True
                    if update:
                        stock.update(bar)
                    else:
                        stock.append(bar)
                    self.__event.emit(bar.dateTime,bar)
        return ret

    def __getitem__(self, code):
        return self.__stocks[code]

    def getNextBar(self,code,frequency):
        '''
        获取股票某个周期下的下一个Bar
        :param code:
        :param frequency:
        :return:返回一个tuple，True表示update bar，False表示new bar
        '''
        raise NotImplementedError()

    def addStock(self,stock):
        '''
        不支持动态添加技术指标，添加股票时，要设置好技术指标
        不支持动态添加股票，一开始就要将股票添加进来
        '''
        self.__stocks[stock.code] = stock







