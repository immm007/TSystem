from pyalgotrade import bar
from pyalgotrade.dataseries.bards import BufferedBarDataSeries
from pyalgotrade.technical import ma
from pyalgotrade.stock.wangyi import DayDataHelper


class Stock:
    '''
    strategy, feed and broker should use this interface instead of dataseries
    需维护日线以及分钟数据库，使用网易数据源和新浪数据源
    新浪数据源分钟级别数据仅支持1023个节点，因此新添加的股票需要从通达信导入历史数据
    使用前复权数据，不要复权日当天交易该之股票
    周线及以上周期需特殊处理，暂不支持
    不支持动态修改分析周期
    '''
    def __init__(self, code, periods=None):
        self.__code = code
        if periods is None:
            #没有找到在线1分钟数据源，采用5分钟为最小周期
            periods = (bar.Frequency.FIVE_MINUTE,
                       bar.Frequency.FIFTEEN_MINUTE,
                       bar.Frequency.THIRTY_MINUTE,
                       bar.Frequency.HOUR,
                       bar.Frequency.DAY)
        self.__periods = periods
        self.__barSeries = {frequency:BufferedBarDataSeries() for frequency in periods}
        self._lastPrice =None
        self.__highLimit = None
        self.__lowLimit = None
        self.__percentChange =None
        self.__dayDataHelper = DayDataHelper()

    def __getitem__(self, period):
        '''
        返回特定分析周期的数据序列
        :param period:
        :return:
        '''
        return self.__barSeries[period]

    def append(self,bar:bar.BasicBar):
        self.__barSeries[bar.getFrequency()].append(bar.getDateTime(), bar)

    def update(self,bar:bar.BasicBar):
        self.__barSeries[bar.getFrequency()].update(bar.getDateTime(), bar)

    @property
    def code(self):
        return  self.__code

    @property
    def analysisPeriods(self):
        return self.__periods

    def loadData(self,star_date='19900101',end_date = None):
        '''
        首先尝试从数据库中补全历史数据，若有缺失则尝试从网上下载
        :param historyData:
        :return:
        '''
        self.__dayDataHelper.loadBarsToStock(self, star_date, end_date)









