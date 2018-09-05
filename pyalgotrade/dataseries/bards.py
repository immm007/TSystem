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

from pyalgotrade import dataseries
import pandas as pd


class BarDataSeries(dataseries.SequenceDataSeries):
    """A DataSeries of :class:`pyalgotrade.bar.Bar` instances.

    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """

    def __init__(self, maxLen=None):
        super(BarDataSeries, self).__init__(maxLen)
        self.__openDS = dataseries.SequenceDataSeries(maxLen)
        self.__closeDS = dataseries.SequenceDataSeries(maxLen)
        self.__highDS = dataseries.SequenceDataSeries(maxLen)
        self.__lowDS = dataseries.SequenceDataSeries(maxLen)
        self.__volumeDS = dataseries.SequenceDataSeries(maxLen)
        self.__adjCloseDS = dataseries.SequenceDataSeries(maxLen)
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
        assert(dateTime is not None)
        assert(bar is not None)
        bar.setUseAdjustedValue(self.__useAdjustedValues)

        super(BarDataSeries, self).appendWithDateTime(dateTime, bar)

        self.__openDS.appendWithDateTime(dateTime, bar.getOpen())
        self.__closeDS.appendWithDateTime(dateTime, bar.getClose())
        self.__highDS.appendWithDateTime(dateTime, bar.getHigh())
        self.__lowDS.appendWithDateTime(dateTime, bar.getLow())
        self.__volumeDS.appendWithDateTime(dateTime, bar.getVolume())
        self.__adjCloseDS.appendWithDateTime(dateTime, bar.getAdjClose())

        # Process extra columns.
        for name, value in bar.getExtraColumns().items():
            extraDS = self.__getOrCreateExtraDS(name)
            extraDS.appendWithDateTime(dateTime, value)

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


class BufferedBarDataSeries(dataseries.BufferedSequenceDataSeries):
    '''
    带缓存功能的Bar序列
    '''
    def __init__(self,zipMode=True):
        super().__init__()
        self.__zipMode = zipMode
        if not zipMode:
            self.__openDS = dataseries.BufferedSequenceDataSeries()
            self.__highDS = dataseries.BufferedSequenceDataSeries()
            self.__lowDS = dataseries.BufferedSequenceDataSeries()
        self.__closeDS = dataseries.BufferedSequenceDataSeries()
        self.__volumeDS = dataseries.BufferedSequenceDataSeries()

    def append(self, dateTime, bar):

        assert(dateTime is not None)
        assert(bar is not None)

        super().append(dateTime,bar)

        if not self.__zipMode:
            self.__openDS.append(dateTime, bar.open)
            self.__highDS.append(dateTime, bar.high)
            self.__lowDS.append(dateTime, bar.low)
        self.__closeDS.append(dateTime, bar.close)
        self.__volumeDS.append(dateTime, bar.volume)

    def update(self,dateTime,bar):

        assert(dateTime is not None)
        assert(bar is not None)

        super().update(dateTime,bar)

        if not self.__zipMode:
            self.__openDS.update(dateTime, bar.open)
            self.__highDS.update(dateTime, bar.high)
            self.__lowDS.update(dateTime, bar.low)

        self.__closeDS.update(dateTime, bar.close)
        self.__volumeDS.update(dateTime, bar.volume)

    @property
    def openDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the open prices."""
        return self.__openDS

    @property
    def closeDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the close prices."""
        return self.__closeDS

    @property
    def highDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the high prices."""
        return self.__highDS

    @property
    def lowDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the low prices."""
        return self.__lowDS

    @property
    def volumeDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the volume."""
        return self.__volumeDS

    @property
    def zipMode(self):
        return self.__zipMode

    def toDF(self)->pd.DataFrame:
        '''
        大量内存复制操作，应该仅在盘后分析时调用
        :return:
        '''
        if not self.__zipMode:
            data = { 'open':list(iter(self.__openDS)),
                    'clsoe':list(iter(self.__closeDS)),
                     'high':list(iter(self.__highDS)),
                     'low':list(iter(self.__lowDS)),
                     'volume':list(iter(self.__volumeDS)) }
        else:
            data = { 'clsoe':list(iter(self.__closeDS)), 'volume':list(iter(self.__volumeDS)) }

        ret = pd.DataFrame(data, index=self.dateTimes)
        ret.index.name = 'datetime'
        return ret

    def toCSV(self,path):
        self.toDF().to_csv(path)


