from pyalgotrade.utils import addWangyiPrefix,CSVStringHelper
import requests
import csv

class Quoter:
    '''
    网易API接口的封装，所有方法都是静态方法
    '''
    def getDayData(code:str,start_date:str,end_date:str,timeout=5):
        '''
        获取某只股票任意段时间段的日线历史数据,注意是除权数据
        :param code: 股票代码
        :param start_date: 起始日期，格式1990-01-01
        :param end_date: 结束日期，格式2018-01-01
        :return: 返回CSV类
        '''
        url = ("http://quotes.money.163.com/service/chddata.html?"
                                "code={0}&start={1}&end={2}&fields="
                                 "TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
               .format(addWangyiPrefix(code), start_date.replace('-', ''), end_date.replace('-', '')))
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        content =CSVStringHelper(response.text)
#        with open('tmp.csv','w') as f:
#            f.write(content)
        iterator = iter(content)
        #去掉第一行头
        next(iterator)
        return csv.reader(iterator)

    def getAllCloseData(code,period='day',fq=True,timeout=5):
        '''
        获取股票或指数日，周，月度所有时间节点的收盘数据，可选择后复权或除权数据
        :param code:标的代码
        :param period: day, week, month
        :param fq:复权为True，不复权为False
        :return:dict
        '''
        if not fq:
            kline = 'kline'
        else:
            kline = 'klinederc'
        url = 'http://img1.money.126.net/data/hs/%s/%s/times/%s.json' % (kline, period,addWangyiPrefix(code))
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.json()

    def getYearlyDaysData(code,year,period='day',timeout=5):
        '''
        获取股票或指数，某一年度的日，周，月线所有数据，注意是除权数据
        :param year:
        :param period:
        :return:dict
        '''
        url = 'http://img1.money.126.net/data/hs/kline/%s/history/%s/%s.json' % (period, year, addWangyiPrefix(code))
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.json()

    def getTodayCurve(code,timeout=5):
        '''
        获取当日分时图数据，注意是除权数据
        :return:dict
        '''
        url = 'http://img1.money.126.net/data/hs/time/today/%s.json' % addWangyiPrefix(code)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.json()

    def get4DaysCurve(code,timeout=5):
        '''
        获取最近四天（不包括今天）的分时图数据，注意是除权数据
        :return:dict
        '''
        url = 'http://img1.money.126.net/data/hs/time/4days/%s.json' % addWangyiPrefix(code)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.json()