from pyalgotrade.feed import BaseLiveFeed
from pyalgotrade import bar
import requests
import sqlite3
import os
import csv
import datetime

class Quoter:
    '''
    只提供股票和指数的日线历史数据，暂不支持ETF基金
    '''
    def get(self,code:str,start_date:str,end_date:str):
        '''
        获取某只股票日线的某段时间历史数据
        :param code: 股票代码，沪市前缀0，深市前缀1。
        :param start_date: 起始日期，格式19900101
        :param end_date: 结束日期，格式20180101
        :return: 返回CSV类
        '''
        url = ("http://quotes.money.163.com/service/chddata.html?"
                                "code={0}&start={1}&end={2}&fields="
                                 "TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(code,start_date,end_date))
        response = requests.get(url,timeout=5)
        response.raise_for_status()
        content = str(response.content,encoding='gbk')
        with open('tmp.csv','w') as f:
            f.write(content)
        data = content.split('\n')
        iterator = iter(data)
        #去掉第一行头
        next(iterator)
        return csv.reader(iterator)


class DayDataHelper:
    '''
    网易数据源日线数据库接口
    每个股票对应一张表，表名是股票的代码
    数据库数据按日期升序排列，使用前复权
    '''
    def __init__(self):
        self.__path = os.path.join(os.path.join(os.getcwd(),'datasource\\wangyi'),'day.db')
        self.__quoter = Quoter()
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
        end_date = str(datetime.datetime.now().date()).replace('-','')
        data = self.__quoter.get(self.__addPrefix(code),'19900101',end_date)
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
            start_date = datetime.datetime.strftime(start_date,'%Y%m%d')
            data = self.__quoter.get(self.__addPrefix(code), start_date, today.replace('-',''))
            for row in data:
                self.__inset_without_commit(code, *row)
            self.__commit()

    def __getLatestDate(self,code):
        with sqlite3.connect(self.__path) as conn:
            cursor = conn.execute("SELECT DATE FROM '%s' ORDER BY DATE DESC" % code)
            for row in cursor:
                return row[0]
        #空表
        return '1989-12-31'

    def __addPrefix(self,code):
        '''
        :param code:
        :return: 前缀+股票代码 0表示沪市，1表示深市
        '''
        if code[0] == '6':
            return '0'+code
        elif code[0]=='0' or code[0]=='3':
            return '1'+code
        raise RuntimeError('unsupoorted code %s' % code)

    def loadBarsToStock(self,stock,start_date='19910101',end_date=None):
        '''
        将数据库中数据拷贝到全新的stock中，为避免重复拷贝，直接操作Stock
        :param stock:
        :param start_date:
        :param end_date:
        :return:
        '''
        #确保stock是空的
        assert len(stock[bar.Frequency.DAY])==0

        code = stock.code
        if not self.__has_table(code):
            self.__clearAndDownloadToDB(code)
        self.__complementDB(code)

        if end_date is None:
            end_date = datetime.datetime.today().date().strftime('%Y-%m-%d')
        start_date = '-'.join((start_date[0:4], start_date[4:6], start_date[6:8]))

        with sqlite3.connect(self.__path) as conn:
            cmd = "SELECT STATUS,DATE,TCLOSE,HIGH,LOW,TOPEN,VOTURNOVER FROM '{0}' WHERE DATE>='{1}' AND DATE<='{2}' ORDER By DATE ASC ".format(stock.code,start_date,end_date)
            cursor = conn.execute(cmd)
            for row in cursor:
                if row[0]==1:
                    raise RuntimeError('data missing for %s %s'% (row[1],code))
                stock.append(bar.BasicBar(row[1],row[5],row[3],row[4],row[2],row[6],None,bar.Frequency.DAY))




