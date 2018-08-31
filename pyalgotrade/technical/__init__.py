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

from pyalgotrade.utils import collections
from pyalgotrade import dataseries


class EventWindow:
    """An EventWindow class is responsible for making calculation over a moving window of values.

    :param windowSize: The size of the window. Must be greater than 0.
    :type windowSize: int.
    :param dtype: The desired data-type for the array.
    :type dtype: data-type.
    :param skipNone: True if None values should not be included in the window.
    :type skipNone: boolean.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, windowSize, dtype=float):
        assert(windowSize > 0)
        assert(isinstance(windowSize, int))
        # use buffered one instead
        self.__values = collections.BufferedNumPyDeque(windowSize, dtype)
        self.__windowSize = windowSize

    def __iter__(self):
        return iter(self.__values)

    def __len__(self):
        return len(self.__values)

    def __str__(self):
        return str(self.__values)

    def onNewValue(self, dateTime, value):
        assert dateTime is not None and value is not None
        self.__values.append(value)

    def onUpdateValue(self, dateTime, value):
        assert dateTime is not None and value is not None
        self.__values.update(value)

    @property
    def windowSize(self):
        """Returns the window size."""
        return self.__windowSize

    @property
    def data(self):
        return self.__values.data

    def isFull(self):
        return len(self.__values) == self.__windowSize

    def calculate(self):
        """Override to calculate a value using the values in the window."""
        raise NotImplementedError()


class EventBasedFilter(dataseries.BufferedSequenceDataSeries):
    """An EventBasedFilter class is responsible for capturing new values in a :class:`pyalgotrade.dataseries.DataSeries`
    and using an :class:`EventWindow` to calculate new values.

    :param dataSeries: The DataSeries instance being filtered.
    :type dataSeries: :class:`pyalgotrade.dataseries.DataSeries`.
    :param eventWindow: The EventWindow instance to use to calculate new values.
    :type eventWindow: :class:`EventWindow`.
    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """

    def __init__(self, dataSeries, eventWindow, maxLen=None):
        super().__init__()
        self.__dataSeries = dataSeries
        self.__dataSeries.newValueEvent.subscribe(self.__onNewValue)
        self.__dataSeries.updateValueEvent.subscribe(self.__onUpdateValue)
        self.__eventWindow = eventWindow

    def __onNewValue(self, sender,dateTime, value):
        # Let the event window perform calculations.
        self.__eventWindow.onNewValue(dateTime, value)
        # Get the resulting value
        newValue = self.__eventWindow.calculate()
        # Add the new value.
        self.append(dateTime, newValue)

    def __onUpdateValue(self, sender,dateTime, value):
        # Let the event window perform calculations.
        self.__eventWindow.onUpdateValue(dateTime, value)
        # Get the resulting value
        updatedValue = self.__eventWindow.calculate()
        # Add the update value.
        self.update(dateTime, updatedValue)

    @property
    def eventWindow(self):
        return self.__eventWindow
