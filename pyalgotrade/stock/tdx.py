from pyalgotrade.utils.patterns import Singleton
import os
from ctypes import *
from pandas import DataFrame, concat
from pyalgotrade.barfeed import BaseBarFeed
from pyalgotrade.broker import Broker, Order, LimitOrder, OrderEvent, OrderExecutionInfo, IntegerTraits
from datetime import datetime
from pyalgotrade import bar


class TDX(metaclass=Singleton):
    def __init__(self):
        self.__trade = windll.LoadLibrary(os.getcwd()+'\\Trade.dll')
        self.__userID = '10431631'
        self.__password = '121887'
        self.__lastError = None
        self.__clientID = None
        self.__shAccount = None
        self.__szAccount = None
        self.__servers = ['202.99.230.133']
        self.__findFastestServer = False
        self.__errorBuf = create_string_buffer(b'\x00' * 255)
        self.__retBuf = create_string_buffer(b'\x00' * 1024)

    @property
    def lastError(self):
        return self.__lastError

    @property
    def clientID(self):
        return self.__clientID

    @property
    def shAccount(self):
        return self.__shAccount

    @property
    def szAccount(self):
        return self.__szAccount

    def __fliter(self, buf):
        raw = buf.raw
        end = raw.find(b'\x00')
        return str(raw[0:end], encoding='gbk')

    def __setError(self, buf):
        self.__lastError = self.__fliter(self.__errorBuf)

    def __hasError(self):
        return self.__errorBuf.raw.find(b'\x00') != 0

    def __queryData(self, category):
        self.__trade.QueryData(
            c_int(self.__clientID),
            c_int(category),
            self.__retBuf,
            self.__errorBuf)
        if self.__hasError():
            self.__setError(self.__errorBuf)
            return None
        else:
            return self.__parse(self.__fliter(self.__retBuf))

    def __fillAccounts(self):
        df = self.__queryData(5)
        self.__shAccount = df[df['帐号类别'] == '1']['股东代码'].get(0)
        self.__szAccount = df[df['帐号类别'] == '0']['股东代码'].get(0)

    def __parse(self, ret_str):
        cr = ret_str.find('\n')
        header = ret_str[0:cr]
        content = ret_str[cr + 1:]
        keys = [item for item in header.split('\t')]
        values = [[item for item in line.split('\t')] for line in content.split('\n')]
        return DataFrame(values, columns=keys)

    def open(self):
        return self.__trade.OpenTdx()

    def close(self):
        return self.__trade.CloseTdx()

    def logon(self):
        ret = self.__trade.Logon(c_char_p(bytes(self.__servers[0], encoding='ascii')),
                                 c_short(7708),
                                 c_char_p(bytes('6.00', encoding='ascii')),
                                 c_short(0),
                                 c_char_p(bytes(self.__userID, encoding='ascii')),
                                 c_char_p(bytes(self.__userID, encoding='ascii')),
                                 c_char_p(bytes(self.__password, encoding='ascii')),
                                 c_char_p(bytes('', encoding='ascii')),
                                 self.__errorBuf
                                 )
        if ret == -1:
            self.__setError(self.__errorBuf)
            return False
        else:
            self.__clientID = ret
            self.__fillAccounts()
            return True

    def logoff(self):
        if self.__clientID is not None:
            self.__trade.Logoff(self.__clientID)
            self.__clientID = None
            self.__shAccount = None
            self.__szAccount = None

    def queryMoney(self):
        return self.__queryData(0)

    def queryShares(self):
        return self.__queryData(1)

    def queryDelegates(self):
        return self.__queryData(2)

    def queryTransactions(self):
        return self.__queryData(3)

    def queryCanCancel(self):
        return self.__queryData(4)

    def sendOrder(self, buyOrSell, exchangeID, code, price, quantity):
        if exchangeID == 1:
            account = self.__shAccount
        elif exchangeID == 0:
            account = self.__szAccount
        else:
            self.__lastError = 'invalid exchange id'
            return None
        self.__trade.SendOrder(
            c_int(self.__clientID),
            c_int(buyOrSell),
            c_int(0),
            c_char_p(bytes(account, encoding='ascii')),
            c_char_p(bytes(code, encoding='ascii')),
            c_float(price),
            c_int(quantity),
            self.__retBuf,
            self.__errorBuf
        )
        if self.__hasError():
            self.__setError(self.__errorBuf)
            return None
        else:
            return self.__parse(self.__fliter(self.__retBuf))

    def cancelOrder(self, exchangeID, delegateID):
        self.__trade.CancelOrder(
            c_int(self.__clientID),
            c_char_p(bytes(str(exchangeID), encoding='ascii')),
            c_char_p(bytes(delegateID, encoding='ascii')),
            self.__retBuf,
            self.__errorBuf
        )
        if self.__hasError():
            self.__setError(self.__errorBuf)
            return None
        else:
            return self.__parse(self.__fliter(self.__retBuf))

    def getQuotes(self, codes):
        array = (c_char_p * len(codes))(*[c_char_p(bytes(code, encoding='ascii')) for code in codes])
        results = (c_char_p * len(codes))(*[c_char_p(b'\x00' * 1024) for i in range(len(codes))])
        errors = (c_char_p * len(codes))(*[c_char_p(b'\x00' * 255) for i in range(len(codes))])
        self.__trade.GetQuotes(c_int(self.__clientID), array, c_int(len(codes)), results, errors)
        return concat([self.__parse(str(results[i], encoding='gbk')) for i in range(len(codes))])


class LiveBroker(Broker):
    def __init__(self):
        super().__init__()
        self.__tdx = TDX()
        self.__id = 1
        self.__orders = []
        self.__stop = False

    def getCash(self, includeShort=True):
        raise NotImplementedError()

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return LimitOrder(action, instrument, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise NotImplementedError()

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise NotImplementedError()

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        raise NotImplementedError()

    def cancelOrder(self, order):
        raise NotImplementedError()

    def getPositions(self):
        raise NotImplementedError()

    def getActiveOrders(self, instrument=None):
        raise NotImplementedError()

    def __logOrder(self, order):
        '''
        just record it
        :param order: :class: Order
        :return:
        '''
        if order.getAction() == Order.Action.BUY:
            action = 'buy'
        elif order.getAction() == Order.Action.SELL:
            action = 'sell'
        with os.open('papertrade.txt', 'a') as f:
            f.write(action + ':' + order.getInstrument() + 'at' + order.getLimitPrice() + '\n')

    def submitOrder(self, order: LimitOrder):
        if order.isInitial():
            order.setSubmitted(self.__id, datetime.now())
            self.__id += 1
            order.switchState(Order.State.SUBMITTED)
        if order.isSubmitted():
            order.switchState(Order.State.ACCEPTED)
            self.notifyOrderEvent(OrderEvent(order, OrderEvent.Type.ACCEPTED, None))
        self.__orders.append(order)

    def getShares(self, instrument):
        raise NotImplementedError()

    def getInstrumentTraits(self, instrument):
        return IntegerTraits()

    def join(self):
        pass

    def start(self):
        pass

    def stop(self):
        self.__stop = True

    def eof(self):
        if self.__stop:
            return True

    def peekDateTime(self):
        return None

    def dispatch(self):
        ret = False
        for order in self.__orders:
            if order.isAccepted():
                orderExecutionInfo = OrderExecutionInfo(order.getLimitPrice(), order.getQuantity(), 0, datetime.now())
                order.addExecutionInfo(orderExecutionInfo)
                self.notifyOrderEvent(OrderEvent(order, OrderEvent.Type.FILLED, orderExecutionInfo))
                ret = True
        return ret


class LiveFeed(BaseBarFeed):
    # TODO: Deprecated, use sina.LiveFeed instead
    def __init__(self, maxLen=None):
        super().__init__(bar.Frequency.TRADE, maxLen)
        self._tdx = TDX()
        self._stopped = False

    def getNextBars(self):
        df = self._tdx.getQuotes(self.getKeys())
        df.set_index(['证券代码'], inplace=True)
        dt = datetime.now()
        return bar.Bars({code: bar.BasicBar(dt,
                                            float(df['当前价'][code]),
                                            float(df['当前价'][code]),
                                            float(df['当前价'][code]),
                                            float(df['当前价'][code]),
                                            0, None, bar.Frequency.TRADE) for code in df.index})

    def getCurrentDateTime(self):
        return datetime.now()

    def peekDateTime(self):
        return None

    def barsHaveAdjClose(self):
        return False

    def start(self):
        super().start()

    def eof(self):
        return self._stopped

    def stop(self):
        self._stopped = True

    def join(self):
        pass