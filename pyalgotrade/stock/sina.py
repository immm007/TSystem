from pyalgotrade.utils import addSinaPrefix,CSVStringHelper
from pyalgotrade.feed import BaseLiveFeed
import requests


d1 = {'day':'day','open':'open','close':'close','high':'high','low':'low','volume':'volume'}

class Quoter:
    '''
    新浪api封装，所有类都是静态方法
    '''
    def getRTQuote(code,timeout=5):
        url = "http://hq.sinajs.cn/list=%s" % addSinaPrefix(code)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.text

    def getIntraDayQuote(code,period=5,timeout=5):
        '''
        获取5,15,30,60分钟数据，最多1023个数据
        :param period: :type int
        :param timeout:
        :return:list of dict
        '''
        url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?" \
              "symbol=%s&scale=%d&ma=no&datalen=1023" % (addSinaPrefix(code),period)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return eval(response.text,d1)

    def getAllCloseData(code,fq='houfuquan',timeout=5):
        '''
        获取股票上市以来所有收盘价的前复权或后复权，前复权数据貌似不准确
        :param fq:
        :param timeout:
        :return:
        '''
        #TODO 待完成解析功能
        url = "http://finance.sina.com.cn/realstock/company/%s/%s.js" % (addSinaPrefix(code),fq)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        content = next(CSVStringHelper(response.text))
        return content