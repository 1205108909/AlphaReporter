import datetime
import os.path
import warnings

import h5py
import pandas as pd

import Configuration as config
import Log


class HighFrequencyService(object):
    log = Log.get_logger(__name__)

    # 实现兼容自定义路径
    @classmethod
    def set_config_path(cls, path='./config.json'):
        # 配置为高频数据地址
        appconfig = config.Configuration(path)
        cls.tick_file_path = appconfig.highFreqData['stockTick']
        cls.transaction_file_path = appconfig.highFreqData['stockTran']
        cls.order_file_path = appconfig.highFreqData['stockOrder']
        cls.orderqueue_file_path = appconfig.highFreqData['stockOrderQueue']
        cls.k60s_minuteBar = appconfig.highFreqData['stock60MinuterBar']
        cls.tick_index_path = appconfig.highFreqData['IndexLevel2Tick']
        cls.minuteBar_index_path = appconfig.highFreqData['IndexMinuteBarH5']
        cls.tickAlign_file_path = appconfig.highFreqData['stockTickAlign']
        return cls

    @classmethod
    def get_tradable_stock(cls, tradingday):
        """
        read tradble symbol
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame
        """
        h5file = h5py.File(os.path.join(cls.tick_file_path, ''.join([tradingday, '.h5'])), 'r')
        list = []
        for s in h5file.keys():
            list.append(s)
        h5file.close()
        return list

    @classmethod
    def read_tick(cls, symbol, tradingday):
        """
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame Time类型：93003000 int
        """
        with h5py.File(os.path.join(cls.tick_file_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            if len(time) == 0:
                print(f'{symbol} tick is null')
                return pd.DataFrame()
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            turnover = f[symbol]['Turnover']
            matchItem = f[symbol]['MatchItem']
            bsflag = f[symbol]['BSFlag']
            accVolume = f[symbol]['AccVolume']
            accTurnover = f[symbol]['AccTurnover']
            askAvgPrice = f[symbol]['AskAvgPrice']
            bidAvgPrice = f[symbol]['BidAvgPrice']
            totalAskVolume = f[symbol]['TotalAskVolume']
            totalBidVolume = f[symbol]['TotalBidVolume']
            open_p = f[symbol]['Open']
            high = f[symbol]['High']
            low = f[symbol]['Low']
            preClose = f[symbol]['PreClose']

            tick = pd.DataFrame(
                {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover, 'MatchItem': matchItem,
                 'BSFlag': bsflag, 'AccVolume': accVolume, 'AccTurnover': accTurnover, 'AskAvgPrice': askAvgPrice,
                 'BidAvgPrice': bidAvgPrice, 'TotalAskVolume': totalAskVolume, 'TotalBidVolume': totalBidVolume,
                 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})

            for i in range(10):
                tick['BidPrice' + str(i + 1)] = f[symbol]['BidPrice10'][:][:, i]
                tick['AskPrice' + str(i + 1)] = f[symbol]['AskPrice10'][:][:, i]
                tick['BidVolume' + str(i + 1)] = f[symbol]['BidVolume10'][:][:, i]
                tick['AskVolume' + str(i + 1)] = f[symbol]['AskVolume10'][:][:, i]
        return tick

    @classmethod
    def read_stock_align_tick(cls, symbol, tradingday):
        """read_stock_align_tick
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame  Time类型：93003000 int
        """
        with h5py.File(os.path.join(cls.tickAlign_file_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            if len(time) == 0:
                print(f'{symbol} tick is null')
                return pd.DataFrame()
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            turnover = f[symbol]['Turnover']
            matchItem = f[symbol]['MatchItem']
            bsflag = f[symbol]['BSFlag']
            accVolume = f[symbol]['AccVolume']
            accTurnover = f[symbol]['AccTurnover']
            askAvgPrice = f[symbol]['AskAvgPrice']
            bidAvgPrice = f[symbol]['BidAvgPrice']
            totalAskVolume = f[symbol]['TotalAskVolume']
            totalBidVolume = f[symbol]['TotalBidVolume']
            open_p = f[symbol]['Open']
            high = f[symbol]['High']
            low = f[symbol]['Low']
            preClose = f[symbol]['PreClose']

            tick = pd.DataFrame(
                {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover, 'MatchItem': matchItem,
                 'BSFlag': bsflag, 'AccVolume': accVolume, 'AccTurnover': accTurnover, 'AskAvgPrice': askAvgPrice,
                 'BidAvgPrice': bidAvgPrice, 'TotalAskVolume': totalAskVolume, 'TotalBidVolume': totalBidVolume,
                 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})

            for i in range(10):
                tick['BidPrice' + str(i + 1)] = f[symbol]['BidPrice10'][:][:, i]
                tick['AskPrice' + str(i + 1)] = f[symbol]['AskPrice10'][:][:, i]
                tick['BidVolume' + str(i + 1)] = f[symbol]['BidVolume10'][:][:, i]
                tick['AskVolume' + str(i + 1)] = f[symbol]['AskVolume10'][:][:, i]
        return tick

    @classmethod
    def read_transaction(cls, symbol, tradingday):
        """
        read transaction data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame
        """
        with h5py.File(os.path.join(cls.transaction_file_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            functionCode = f[symbol]['FunctionCode']
            orderKind = f[symbol]['OrderKind']
            bsflag = f[symbol]['BSFlag']
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            ask_order = f[symbol]['AskOrder']
            bid_order = f[symbol]['BidOrder']

            transaction = pd.DataFrame(
                {'Time': time, 'FunctionCode': functionCode, 'OrderKind': orderKind, 'Bsflag': bsflag, 'Price': price,
                 'Volume': volume,
                 'AskOrder': ask_order, 'BidOrder': bid_order})

        return transaction

    @classmethod
    def read_order(cls, symbol, tradingday):
        """
        read order data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame
        """
        with h5py.File(os.path.join(cls.order_file_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            order_number = f[symbol]['OrderNumber']
            orderKind = f[symbol]['OrderKind']
            function_code = f[symbol]['FunctionCode']
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']

            order = pd.DataFrame(
                {'Time': time, 'OrderKind': orderKind, 'OrderNumber': order_number, 'Price': price, 'Volume': volume,
                 'FunctionCode': function_code})

        return order

    @classmethod
    def read_orderqueue(cls, symbol, tradingday):
        """
        read orderqueue data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: list [orderqueueitem]
                 orderqueueitem {} dict
        """
        with h5py.File(os.path.join(cls.orderqueue_file_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            side = f[symbol]['Side']
            price = f[symbol]['Price']
            orderItems = f[symbol]['OrderItems']
            abItems = f[symbol]['ABItems']
            abVolume = f[symbol]['ABVolume']

            # for i in range(len(time)):
            #     orderqueueItem = {'Time': time[i], 'Side': side[i], 'Price': price[i], 'OrderItems': orderItems[i],
            #                       'ABItems': abItems[i], 'ABVolume': abVolume[i, :]}
            #     orderqueue.append(orderqueueItem)

            orderqueue = pd.DataFrame(
                {'Time': time, 'Side': side, 'Price': price, 'OrderItems': orderItems, 'ABItems': abItems,
                 'ABVolume': abVolume})
        return orderqueue

    @classmethod
    def get_main_contact(cls, contract, tradingDay):
        """
        获取主力合约文件名
        :param contract: 'IF'、'IC'、'IH' str
        :param '20190228' str
        """
        files = os.listdir(cls.index_future_path + tradingDay)
        fl = pd.DataFrame(files)
        return fl.iloc[(cls.index_future_path + tradingDay + '\\' + fl[fl[0].str.contains(contract)][0]).apply(
            os.path.getsize).idxmax()][0]

    @classmethod
    def read_fut_tick(cls, contract, tradingDay):
        """
        获取期货地址
        :param contract: 'IF'、'IC'、'IH' str
        :param '20190228' str
        """
        symbol = cls.get_main_contact(contract, tradingDay)
        tick = pd.read_csv(cls.index_future_path + tradingDay + '\\' + symbol)
        return tick[(tick['Time'] >= 90000000) & (tick['Time'] <= 151500000)]

    @classmethod
    def read_h5_key(cls, constant, tradingDay):
        try:
            symbols = []
            with h5py.File(os.path.join(cls.dictTypeAndPath[constant], tradingDay + '.h5'), 'r') as f:
                for symbol in f.keys():
                    symbols.append(symbol)
            return symbols
        except Exception as e:
            cls.log.error(f'{constant}--{tradingDay} read_h5_key is Fail')
            cls.log.error(e)

    @classmethod
    def read_index_tick(cls, symbol, tradingday):
        """read_index_tick
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame   Time的类型92519000 int
        """
        with h5py.File(os.path.join(cls.tick_index_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            if len(time) == 0:
                print(f'{symbol} tick is null')
                return pd.DataFrame()
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            turnover = f[symbol]['Turnover']

            accVolume = f[symbol]['AccVolume']
            accTurnover = f[symbol]['AccTurnover']

            open_p = f[symbol]['Open']
            high = f[symbol]['High']
            low = f[symbol]['Low']
            preClose = f[symbol]['PreClose']

            tick = pd.DataFrame(
                {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover, 'AccVolume': accVolume,
                 'AccTurnover': accTurnover, 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})
        return tick

    @classmethod
    def read_index_minuteBar(cls, symbol, tradingday):
        """read_index_minuteBar
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame  Time的类型: 930 int
        """
        with h5py.File(os.path.join(cls.minuteBar_index_path, ''.join([tradingday, '.h5'])), 'r') as f:
            if symbol not in f.keys():
                return None
            time = f[symbol]['Time']
            if len(time) == 0:
                print(f'{symbol} tick is null')
                return pd.DataFrame()
            close = f[symbol]['Close']
            volume = f[symbol]['Volume']
            turnover = f[symbol]['Turnover']
            avg = f[symbol]['Avg']
            open_p = f[symbol]['Open']
            high = f[symbol]['High']
            low = f[symbol]['Low']
            preClose = f[symbol]['PreClose']

            tick = pd.DataFrame(
                {'Time': time, 'Close': close, 'Volume': volume, 'Turnover': turnover, 'Avg': avg,
                 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})
        return tick

    @classmethod
    def read_stock_minuteBar_process(cls, symbol, tradingday):
        """read_stock_minuteBar_process
        read_stock_minuteBar_process 用于处理获取的h5数据
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame Time的类型：'093000' str
        """
        df = cls.read_stock_minuteBar(symbol, tradingday)
        if df.shape[0] == 0:
            return pd.DataFrame()
        df['Time'] = df['Time'] * 100
        df['Time'] = df['Time'].astype('str')
        df['Time'] = df['Time'].map(lambda x: x.rjust(6, '0'))
        df['Value'] = round(df['Close'] / df['PreClose'] - 1, 4)
        df = df[df['Time'] >= '092400']
        date = tradingday

        dt_113000 = pd.to_datetime(date + ' 11:31:00')

        df['datetime'] = tradingday
        df['datetime'] = df['datetime'].str.cat(df['Time'], sep=' ')
        df['datetime'] = pd.to_datetime(df['datetime'])

        df['rdatetime'] = df['datetime']
        rowInd = df['rdatetime'] >= dt_113000
        df.loc[rowInd, 'rdatetime'] = df.loc[rowInd, 'rdatetime'] - datetime.timedelta(minutes=90)
        df = df[df['Volume'] != 0]

        # set color
        df['color'] = 'red'
        shifted = df['Close'].shift(1)
        dt_092400 = pd.to_datetime(tradingday + ' 09:24:00')
        rowInd = df['rdatetime'] == dt_092400
        df.loc[rowInd, 'Close'] = df.loc[rowInd, 'PreClose']
        df.loc[df['Close'] < shifted, 'color'] = 'green'
        df.loc[df['Close'] == shifted, 'color'] = 'white'
        df = df[df['Time'] >= '093000']

        return df

    @classmethod
    def read_stock_minuteBar(cls, symbol, tradingday):
        """read_stock_minuteBar
        read kdata
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame Time类型:925 int
        """
        try:
            with h5py.File(os.path.join(cls.k60s_minuteBar, ''.join([tradingday, '.h5'])), 'r') as f:
                if symbol not in f.keys():
                    cls.log.warn(f'{symbol} is not found in {cls.k60s_minuteBar}/{tradingday}')
                    return pd.DataFrame()
                t = f[symbol]['Time']
                o = f[symbol]['Open']
                h = f[symbol]['High']
                l = f[symbol]['Low']
                c = f[symbol]['Close']
                v = f[symbol]['Volume']
                to = f[symbol]['Turnover']
                avg = f[symbol]['Avg']
                pc = f[symbol]['PreClose']
                i = f[symbol]['Item']
                bi = f[symbol]['BuyItem']
                bv = f[symbol]['BuyVolume']
                si = f[symbol]['SellItem']
                sv = f[symbol]['SellVolume']
                ci = f[symbol]['CancelItem']
                cv = f[symbol]['CancelVolume']

                data = pd.DataFrame(
                    {'symbol': symbol, 'Time': t, 'Open': o, 'High': h, 'Low': l, 'Close': c, 'Avg': avg, 'Volume': v,
                     'Turnover': to, 'PreClose': pc, 'Item': i, 'BuyItem': bi, 'BuyVolume': bv, 'SellItem': si,
                     'SellVolume': sv, 'CancelItem': ci, 'CancelVolume': cv})
                return data
        except Exception as e:
            cls.log.error(f'{tradingday}---{symbol} read_kLine_more got Wrong')
            cls.log.error(e)
            return pd.DataFrame()

    @classmethod
    def read_index_minuteBar_csv(cls, symbol, tradingday):
        """:cvar read index minuteBar from csv
            Time类型：'093000' str
            此方法已废弃，替代方法为read_index_minuteBar_process
        """
        warnings.warn("read_index_minuteBar_csv is deprecated,please use 'read_index_minuteBar_process' instead", DeprecationWarning)
        try:
            df = pd.read_csv(os.path.join(cls.minuteBar_index_path, tradingday, f'{symbol}' + '.csv'),
                             parse_dates=['Time'])
            df['Value'] = round(df['Close'] / df['PreClose'] - 1, 4)
            df = df[df['Time'] >= '092400']
            dt_113000 = pd.to_datetime(tradingday + ' 11:31:00')

            df['datetime'] = tradingday
            df['datetime'] = df['datetime'].str.cat(df['Time'], sep=' ')
            df['datetime'] = pd.to_datetime(df['datetime'])

            df['rdatetime'] = df['datetime']
            rowInd = df['rdatetime'] >= dt_113000
            df.loc[rowInd, 'rdatetime'] = df.loc[rowInd, 'rdatetime'] - datetime.timedelta(minutes=90)
            df = df[df['Volume'] != 0]

            # set color
            df['color'] = 'red'
            shifted = df['Close'].shift(1)
            dt_092400 = pd.to_datetime(tradingday + ' 09:24:00')
            rowInd = df['rdatetime'] == dt_092400
            df.loc[rowInd, 'Close'] = df.loc[rowInd, 'PreClose']
            df.loc[df['Close'] < shifted, 'color'] = 'green'
            df.loc[df['Close'] == shifted, 'color'] = 'white'
            df = df[df['Time'] >= '093000']

            return df
        except Exception as e:
            cls.log.error(f'{tradingday}---{symbol} read_KLine_csv got Wrong')
            cls.log.error(e)
            return pd.DataFrame()

    @classmethod
    def read_index_minuteBar_process(cls, symbol, tradingday):
        """index_minuteBar的加工数据
            Time:类型 '092400' str
        """
        try:
            df = cls.read_index_minuteBar(symbol, tradingday)
            # df = pd.read_in(os.path.join(cls.minuteBar_index_path, tradingday, f'{symbol}' + '.csv'),
            #                  parse_dates=['Time'])
            df['Value'] = round(df['Close'] / df['PreClose'] - 1, 4)
            df['Time'] = df['Time'] * 100
            df['Time'] = df['Time'].astype('str')
            df['Time'] = df['Time'].map(lambda x: x.rjust(6, '0'))
            df = df[df['Time'] >= '092400']
            dt_113000 = pd.to_datetime(tradingday + ' 11:31:00')

            df['datetime'] = tradingday
            df['datetime'] = df['datetime'].str.cat(df['Time'], sep=' ')
            df['datetime'] = pd.to_datetime(df['datetime'])

            df['rdatetime'] = df['datetime']
            rowInd = df['rdatetime'] >= dt_113000
            df.loc[rowInd, 'rdatetime'] = df.loc[rowInd, 'rdatetime'] - datetime.timedelta(minutes=90)
            df = df[df['Volume'] != 0]

            # set color
            df['color'] = 'red'
            shifted = df['Close'].shift(1)
            dt_092400 = pd.to_datetime(tradingday + ' 09:24:00')
            rowInd = df['rdatetime'] == dt_092400
            df.loc[rowInd, 'Close'] = df.loc[rowInd, 'PreClose']
            df.loc[df['Close'] < shifted, 'color'] = 'green'
            df.loc[df['Close'] == shifted, 'color'] = 'white'
            df = df[df['Time'] >= '093000']

            return df
        except Exception as e:
            cls.log.error(f'{tradingday}---{symbol} read_KLine_csv got Wrong')
            cls.log.error(e)
            return pd.DataFrame()


if __name__ == '__main__':
    # stock H5格式的tick数据
    symbols = ['600000.sh', '000002.sz']
    # tradingdays = ['20180302','20180330','20180402','20180412','20180418','20180601','20190923','20190530']
    tradingdays = ['20170103','20170104','20170309','20170323','20170821' , '20171227']

    for symbol in symbols:
        for tradingday in tradingdays:
            print('-------------------------------')
            df_stock_tick = HighFrequencyService.set_config_path("./config.json").read_tick(symbol, tradingday)
            print(f'{tradingday}__{symbol} count is {df_stock_tick.shape[0]}')

            df_stock_transaction = HighFrequencyService.set_config_path("./config.json").read_transaction(symbol, tradingday)
            print(f'{tradingday}__{symbol} count is {df_stock_transaction.shape[0]}')

            if symbol.endswith('.sz'):
                df_stock_order = HighFrequencyService.set_config_path("./config.json").read_order(symbol, tradingday)
                print(f'{tradingday}__{symbol} count is {df_stock_order.shape[0]}')

            df_stock_orderqueue = HighFrequencyService.set_config_path("./config.json").read_orderqueue(symbol, tradingday)
            print(f'{tradingday}__{symbol} count is {df_stock_orderqueue.shape[0]}')

    # # 读取index H5格式的tick数据
    # df_index_tick = HighFrequencyService.set_config_path("./config.json").read_index_tick('801010.si', '20200102')
    # # df_index_tick.to_csv('C:\\Users\\hc01\\Desktop\\temp\\20200827_801010_tick.si.csv')
    # #
    # # # 读取index H5格式的minuteBar数据
    # df_index_minuteBar_h5 = HighFrequencyService.set_config_path("./config.json").read_index_minuteBar('801010.si', '20201019')
    # # df_index_minuteBar_h5.to_csv('C:\\Users\\hc01\\Desktop\\temp\\20200827_801010_minuteBar.si.csv')
    # #
    # # 读取stock H5格式的minuteBar数据
    # # df_stock_minuteBar_h5 = HighFrequencyService.set_config_path("./config.json").read_stock_minuteBar_process('600000.sh', '20200827')
    # # df_stock_minuteBar_h5.to_csv('C:\\Users\\hc01\\Desktop\\temp\\20200827_600000.sh.csv')
    # #
    # # # 读取index csv格式的minuteBar数据
    # df_stock_minuteBar_csv = HighFrequencyService.set_config_path("./config.json").read_index_minuteBar_csv('801010.si', '20200827')
    # df_stock_minuteBar_csv.to_csv('C:\\Users\\hc01\\Desktop\\temp\\20200827_801010.si.csv')
