from pyalgotrade.dataseries.bards import BufferedBarDataSeries
from pyalgotrade.stock import wangyi,sina
import datetime
import os
import sqlite3


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


class DBInterface:
    '''
    网易数据源日线数据库接口
    每个股票对应一张表，表名是股票的代码
    数据库数据按日期升序排列，使用前复权
    '''
    def __init__(self):
        self.__path = os.path.join(os.path.join(os.getcwd(),'datasource\\wangyi'),'day.db')
        self.__conn = None

    def __get_tables(self):
        with sqlite3.connect(self.__path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor]

    def __has_table(self, code):
        return code in self.__get_tables()

    def __create_table(self, code):
        if not self.__has_table(code):
            with sqlite3.connect(self.__path) as conn:
                conn.execute('CREATE TABLE "{0}"('
                      'STATUS INTEGER NOT NULL,'
                      'DATE TEXT PRIMARY KEY NOT NULL,'
                      'CODE TEXT NOT NULL,'
                      'NAME TEXT NOT NULL,'
                      'TCLOSE REAL, '
                      'HIGH REAL,'
                      'LOW REAL,'
                      'TOPEN REAL,'
                      'LCLOSE REAL,'
                      'CHG REAL,'
                      'PCHG REAL,'
                      'TURNOVER REAL,'
                      'VOTURNOVER REAL,'
                      'VATURNOVER REAL,'
                      'TCAP REAL,'
                      'MCAP REAL);'.format(code))
                conn.commit()

    def __inset_without_commit(self, code, *args):
        if len(args) == 0:
            return
        try:
            if self.__conn is None:
                self.__conn = sqlite3.connect(self.__path)
            cmd = "INSERT INTO '{0}'(STATUS,DATE,CODE,NAME,TCLOSE,HIGH,LOW,TOPEN,LCLOSE,CHG,PCHG,TURNOVER,VOTURNOVER,VATURNOVER,TCAP,MCAP) \
                VALUES({1},'{2}',{3}','{4}',{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16})".format(code,0,*args)
            self.__conn.execute(cmd)
        except sqlite3.OperationalError as e:
            #数据不全，数据库添加标记
            cmd = "INSERT INTO '{0}'(STATUS,DATE,CODE,NAME) \
                VALUES({1},'{2}',{3}','{4}')".format(code,1,args[0],args[1],args[2])
            self.__conn.execute(cmd)

    def __commit(self):
        if self.__conn is not None:
            self.__conn.commit()
            self.__conn.close()
            self.__conn = None

    def __clear_data(self, code):
        if self.__has_table(code):
            with sqlite3.connect(self.__path) as conn:
                conn.execute("DELETE FROM '{0}'".format(code))
                conn.commit()

    def __clearAndDownloadToDB(self, code):
        '''
        会先抹掉表中所有数据，谨慎使用!
        一般情况下数据库数据应该是正确的，这个函数应近在特殊情况下被调用（比如初始状态或有除权发生）
        '''
        self.__create_table(code)
        self.__clear_data(code)
        #下载到今天为止的所有日线数据
        end_date = str(datetime.datetime.now().date())
        data = wangyi.Quoter.getDayData(code, '1990-01-01', end_date)
        for row in data:
            self.__inset_without_commit(code, *row)
        self.__commit()

    def __complementDB(self,code):
        '''
        补充数据库缺失的数据,假定之前的数据库没有缺失
        :param code:
        :return:
        '''
        latestDate = self.__getLatestDate(code)
        today = str(datetime.datetime.now().date())
        #有段时间没开程序了
        if latestDate<today:
            start_date = datetime.datetime.strptime(latestDate,'%Y-%m-%d')+datetime.timedelta(days=1)
            start_date = datetime.datetime.strftime(start_date,'%Y-%m-%d')
            data = wangyi.Quoter.getDayData(code, start_date, today)
            for row in data:
                #插入会破坏数据库内部顺序，总是排序取出，不要依赖内部顺序
                self.__inset_without_commit(code, *row)
            self.__commit()

    def __getLatestDate(self,code):
        with sqlite3.connect(self.__path) as conn:
            cursor = conn.execute("SELECT DATE FROM '%s' ORDER BY DATE DESC" % code)
            for row in cursor:
                return row[0]
        #空表
        return '1989-12-31'

    def loadDayDataToStock(self, stock, start_date='1990-01-01', end_date=None):
        '''
        将数据库中数据拷贝到全新的stock中，为避免重复拷贝，直接操作Stock
        :param stock:
        :param start_date:
        :param end_date:
        :return:
        '''
        #确保stock是空的
        assert len(stock[Frequency.DAY])==0

        code = stock.code
        if not self.__has_table(code):
            self.__clearAndDownloadToDB(code)
        self.__complementDB(code)

        if end_date is None:
            end_date = datetime.datetime.today().date().strftime('%Y-%m-%d')
        with sqlite3.connect(self.__path) as conn:
            cmd = "SELECT STATUS,DATE,TCLOSE,HIGH,LOW,TOPEN,VOTURNOVER FROM '{0}' WHERE DATE>='{1}' AND DATE<='{2}' ORDER By DATE ASC ".format(code,start_date,end_date)
            cursor = conn.execute(cmd)
            for row in cursor:
                if row[0]==1:
                    raise RuntimeError('data missing for %s %s'% (row[1],code))
                stock.append(Bar(row[1],row[5],row[3],row[4],row[2],row[6],Frequency.DAY))


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
            #TODO 评估是否需要1分钟的数据源
            periods = (Frequency.FIVE_MINUTE,
                       Frequency.FIFTEEN_MINUTE,
                       Frequency.THIRTY_MINUTE,
                       Frequency.HOUR,
                       Frequency.DAY)
        self.__periods = periods
        self.__barSeries = {frequency:BufferedBarDataSeries() for frequency in periods}
        self._lastPrice =None
        self.__highLimit = None
        self.__lowLimit = None
        self.__percentChange =None
        self.__wangyiDB = DBInterface()

    def __getitem__(self, period):
        '''
        返回特定分析周期的数据序列
        :param period:
        :return:
        '''
        return self.__barSeries[period]

    def append(self,bar):
        self.__barSeries[bar.frequency].append(bar.dateTime, bar)

    def update(self,bar):
        self.__barSeries[bar.frequency].update(bar.dateTime, bar)

    @property
    def code(self):
        return  self.__code

    @property
    def analysisPeriods(self):
        return self.__periods

    def loadData(self,star_date='1990-01-01',end_date = None):
        #TODO 暂时只在线载入日内K线，先不维护数据库
        for frequency in self.__periods:
            assert not self.__barSeries[frequency].isBuffering()
            if frequency==Frequency.DAY:
                self.__wangyiDB.loadDayDataToStock(self, star_date, end_date)
            else:
                for d in sina.Quoter.getIntraDayQuote(self.__code,frequency/60):
                    bar = Bar(d['day'],d['open'],d['high'],d['low'],d['close'],d['volume'],frequency)
                    self.__barSeries[frequency].append(bar.dateTime,bar)

    def save(self):
        for f in self.__barSeries:
            self.__barSeries[f].toCSV('%s_%s.csv'% (self.__code,f))
