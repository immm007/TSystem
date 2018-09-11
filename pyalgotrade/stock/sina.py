from pyalgotrade.utils import addSinaPrefix,CSVStringHelper
from pyalgotrade.feed import BaseLiveFeed
from pyalgotrade.stock.bar import Frequency,Bar
import requests


d1 = {'day':'day','open':'open','close':'close','high':'high','low':'low','volume':'volume'}

headers1 = {
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Host': 'money.finance.sina.com.cn',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
}

class Quoter:
    cookies = {5:{},15:{},30:{},60:{}}
    '''
    新浪api封装，所有类都是静态方法
    '''
    def getRTQuote(code,timeout=5):
        url = "http://hq.sinajs.cn/list=%s" % addSinaPrefix(code)
        response = requests.get(url,timeout=timeout)
        response.raise_for_status()
        return response.text.split(',')

    def getIntraDayQuote(code,period=5,timeout=5):
        '''
        获取5,15,30,60分钟数据，只能获取不到300条数据，似乎并不是1023条
        :param period: :type int
        :param timeout:
        :return:list of dict
        '''
        url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?" \
              "symbol=%s&scale=%d&ma=no&datalen=1023" % (addSinaPrefix(code),period)
        response = requests.get(url, timeout=timeout, headers=headers1, cookies=Quoter.cookies[period])
        if response.cookies:
            Quoter.cookies[period] = response.cookies
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


class LiveFeed(BaseLiveFeed):
    def getNextBar(self,code,frequency):
        stock = self[code]
        last_bar = stock[frequency][-1]
        if frequency==Frequency.DAY:
            l = Quoter.getRTQuote(code)
            bar = Bar(l[-3],float(l[1]),float(l[4]),float(l[5]),float(l[3]),int(l[8]),frequency)
            if bar.dateTime==last_bar.dateTime:
                #数据未更新
                if bar == last_bar:
                    return None,None
                return bar,True
            elif bar.dateTime>last_bar.dateTime:
                return bar,False
            else:
                raise RuntimeError("new bar's datetime cannot be less than old one")
        else:
            l = Quoter.getIntraDayQuote(code,frequency/60)
            bar = l[-1]
            bar = Bar(bar['day'],float(bar['open']),float(bar['high']),float(bar['low']),
                      float(bar['close']),int(bar['volume']),frequency)
            if bar.dateTime == last_bar.dateTime:
                assert bar.volume>=last_bar.volume,(str(last_bar),str(bar))
                #数据未更新
                if bar == last_bar:
                    return None,None
                return bar,True
            elif bar.dateTime > last_bar.dateTime:
                #有新的bar出现，看下上一个bar是否需要做最后更新
                bar2 = l[-2]
                bar2 = Bar(bar2['day'], float(bar2['open']), float(bar2['high']), float(bar2['low']),
                           float(bar2['close']), int(bar2['volume']), frequency)
                assert bar2.dateTime==last_bar.dateTime
                #若前一个bar状态有更新则强制改变buffer状态
                if bar2 != last_bar:
                    print('漏更新')
                    stock.append(last_bar)
                return bar,False
            else:
                 raise RuntimeError("new bar's datetime %s cannot be less than old one %s" %(bar.dateTime,last_bar.dateTime))




