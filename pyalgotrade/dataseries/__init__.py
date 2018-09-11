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
from pyalgotrade.utils import collections
import pandas as pd

DEFAULT_MAX_LEN = 1024


def get_checked_max_len(maxLen):
    if maxLen is None:
        maxLen = DEFAULT_MAX_LEN
    if not maxLen > 0:
        raise Exception("Invalid maximum length")
    return maxLen


# It is important to inherit object to get __getitem__ to work properly.
# Check http://code.activestate.com/lists/python-list/621258/
class DataSeries(object, metaclass=abc.ABCMeta):
    """Base class for data series.

    .. note::
        This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def __len__(self):
        """Returns the number of elements in the data series."""
        raise NotImplementedError()

    def __getitem__(self, key):
        """Returns the value at a given position/slice. It raises IndexError if the position is invalid,
        or TypeError if the key type is invalid."""
        if isinstance(key, slice):
            return [self[i] for i in range(*key.indices(len(self)))]
        elif isinstance(key, int):
            if key < 0:
                key += len(self)
            if key >= len(self) or key < 0:
                raise IndexError("Index out of range")
            return self.getValueAbsolute(key)
        else:
            raise TypeError("Invalid argument type")

    # This is similar to __getitem__ for ints, but it shouldn't raise for invalid positions.
    @abc.abstractmethod
    def getValueAbsolute(self, pos):
        raise NotImplementedError()

    @abc.abstractmethod
    def getDateTimes(self):
        """Returns a list of :class:`datetime.datetime` associated with each value."""
        raise NotImplementedError()


class SequenceDataSeries(DataSeries):
    """A DataSeries that holds values in a sequence in memory.

    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """

    def __init__(self, maxLen=None):
        super(SequenceDataSeries, self).__init__()
        maxLen = get_checked_max_len(maxLen)

        self.__newValueEvent = observer.Event()
        self.__values = collections.ListDeque(maxLen)
        self.__dateTimes = collections.ListDeque(maxLen)

    def __len__(self):
        return len(self.__values)

    def __getitem__(self, key):
        return self.__values[key]

    def setMaxLen(self, maxLen):
        """Sets the maximum number of values to hold and resizes accordingly if necessary."""
        self.__values.resize(maxLen)
        self.__dateTimes.resize(maxLen)

    def getMaxLen(self):
        """Returns the maximum number of values to hold."""
        return self.__values.getMaxLen()

    # Event handler receives:
    # 1: Dataseries generating the event
    # 2: The datetime for the new value
    # 3: The new value
    def getNewValueEvent(self):
        return self.__newValueEvent

    def getValueAbsolute(self, pos):
        ret = None
        if pos >= 0 and pos < len(self.__values):
            ret = self.__values[pos]
        return ret

    def append(self, value):
        """Appends a value."""
        self.appendWithDateTime(None, value)

    def appendWithDateTime(self, dateTime, value):
        """
        Appends a value with an associated datetime.

        .. note::
            If dateTime is not None, it must be greater than the last one.
        """

        if dateTime is not None and len(self.__dateTimes) != 0 and self.__dateTimes[-1] >= dateTime:
            raise Exception("Invalid datetime. It must be bigger than that last one")

        assert(len(self.__values) == len(self.__dateTimes))
        self.__dateTimes.append(dateTime)
        self.__values.append(value)

        self.getNewValueEvent().emit(self, dateTime, value)

    def getDateTimes(self):
        return self.__dateTimes.data()


class BufferedSequenceDataSeries:
    def __init__(self):
        self.__newValueEvent = observer.Event()
        self.__updateValueEvent = observer.Event()
        self.__values = collections.BufferedList()
        self.__dateTimes = collections.BufferedList()

    def __len__(self):
        return len(self.__values)

    def __iter__(self):
        return iter(self.__values)

    def __getitem__(self, item):
        return self.__values[item]

    @property
    def newValueEvent(self):
        return self.__newValueEvent

    @property
    def updateValueEvent(self):
        return self.__updateValueEvent

    @property
    def dateTimes(self):
        return self.__dateTimes

    def isBuffering(self):
        return self.__values.isBuffering() or self.__dateTimes.isBuffering()

    def append(self, dateTime,value):
        """Appends a value."""
        assert dateTime is not None
        #技术指标可以为None
        #assert value is not None
        if self.__dateTimes:
            if self.__dateTimes[-1] > dateTime:
                raise Exception("Invalid datetime. It %s must be bigger than that last one %s" % (self.__dateTimes[-1],dateTime))
            if self.__dateTimes[-1]==dateTime and not self.isBuffering():
                raise Exception("Invalid datetime. It %s must be under buffering for the same datetime %s" % (self.__dateTimes[-1],dateTime))
        assert len(self.__values) == len(self.__dateTimes)
        self.__dateTimes.append(dateTime)
        self.__values.append(value)
        self.__newValueEvent.emit(self, dateTime, value)

    def update(self,dateTime,value):
        assert dateTime is not None
        #技术指标可以为None
        #assert value is not None
        if self.__dateTimes and self.__dateTimes[-1] != dateTime:
            raise Exception("Invalid datetime. It %s must be bigger than that last one %s" % (self.__dateTimes[-1],dateTime))
        self.__dateTimes.update(dateTime)
        self.__values.update(value)
        self.__updateValueEvent.emit(self,dateTime,value)


