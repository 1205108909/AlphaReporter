import pymssql
import pandas as pd
import h5py
import sys
import Log
import os
import numpy as np
from configparser import RawConfigParser

from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader
from DataSender.EmailHelper import EmailHelper

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class AlgoTradeReporter(object):
    def __init__(self, start, end, clientIds):
        self.logger = Log.get_logger(__name__)
        self.tick_path = "Y:/Data/h5data/stock/tick/"
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)
        clientIDs = list(clientIds.split(';'))
        self.email = EmailHelper.instance()

        self.run(tradingdays, clientIDs)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                print(e)

    def get_receiveList(self, clientId):
        accountIds = []
        clientIds = []
        clientName = []
        email = []
        repsentEmail = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from Clients where clientId = \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    accountIds.append(row['accountId'])
                    clientIds.append(row['clientId'])
                    clientName.append(row['clientName'])
                    email.append(row['email'])
                    repsentEmail.append(row['repsentEmail'])

        data = pd.DataFrame({'accountId': accountIds, 'clientId': clientIds, 'clientName': clientName, 'email': email,
                             'repsentEmail': repsentEmail})
        return data

    def get_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        symbol = []
        side = []
        effectiveTime = []
        expireTime = []
        avgprice = []
        cumQty = []
        slipageByVwap = []
        price = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId = \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    avgprice.append(row['avgPrice'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    price.append(row['price'])

        data = pd.DataFrame({'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime, 'expireTime': expireTime,
                             'avgprice': avgprice, 'cumQty': cumQty, 'slipageByVwap': slipageByVwap, 'price': price})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgprice'] = data['avgprice'].astype('float')
        data['slipageByVwap'] = data['slipageByVwap'].astype('float')
        data['turnover'] = data['avgprice'] * data['cumQty']
        return data

    def read_symbol_tick(self, tradingday, symbol):
        with h5py.File(os.path.join(self.tick_path + tradingday + ".h5"), 'r') as h5file:
            if symbol in h5file.keys():
                df_tick_symbol = pd.DataFrame({'Time': h5file[symbol]['Time'][:],
                                               'Price': h5file[symbol]['Price'][:],
                                               'AccTurnover': h5file[symbol]['AccTurnover'][:],
                                               'AccVolume': h5file[symbol]['AccVolume'][:],
                                               'Volume': h5file[symbol]['Volume'][:],
                                               'BSFlag': h5file[symbol]['BSFlag'][:],
                                               'BidAvgPrice': h5file[symbol]['BidAvgPrice'][:],
                                               'High': h5file[symbol]['High'][:],
                                               'Low': h5file[symbol]['Low'][:],
                                               'MatchItem': h5file[symbol]['MatchItem'][:],
                                               'Open': h5file[symbol]['Open'][:],
                                               'PreClose': h5file[symbol]['PreClose'][:],
                                               'TotalAskVolume': h5file[symbol]['TotalAskVolume'][:],
                                               'TotalBidVolume': h5file[symbol]['TotalBidVolume'][:],
                                               'Turnover': h5file[symbol]['Turnover'][:],
                                               'AskAvgPrice': h5file[symbol]['AskAvgPrice'][:]})
                return df_tick_symbol
            else:
                self.logger.warn("there is no TickData (" + symbol + ") in h5 file, please check your data")
                return pd.DataFrame()

    def getTickDataBySymbol(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        df_tick_symbol = self.read_symbol_tick(tradingDay, symbol)
        if price == 0:
            return df_tick_symbol[(df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                    df_tick_symbol['Volume'] > 0)]
        else:
            if side == 'Buy' or side == 1:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0) & (df_tick_symbol['Price'] <= price)]
            else:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0) & (df_tick_symbol['Price'] >= price)]

    def getTWAP(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        data = self.getTickDataBySymbol(tradingDay, symbol, startTime, endTime, price, side)
        if data.size > 0:
            return data.Price.sum() / data.Volume.count()
        else:
            return 0

    def cal_cancel_ratio(self, tradingday, clientId):
        orderStatus = []
        cumQty = []
        Qty = []
        leavesQty = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select b.orderStatus,b.cumQty,b.Qty,b.leavesQty from ClientOrderView a RIGHT JOIN ExchangeOrderView b on a.orderId = b.orderId where a.tradingDay = \'{tradingday}\' and a.clientId = \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    orderStatus.append(row['orderStatus'])
                    cumQty.append(row['cumQty'])
                    Qty.append(row['Qty'])
                    leavesQty.append(row['leavesQty'])

        data = pd.DataFrame({'orderStatus': orderStatus, 'cumQty': cumQty, 'Qty': Qty, 'leavesQty': leavesQty})
        return data

    def run(self, tradingDays, clientIds):
        def cal_twap(tradingDay, effectiveTime, expireTime, symbol, price, side):
            effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'{tradingDay}-{effectiveTime}-{expireTime}-{symbol}-{price}-{side}')
            twap = self.getTWAP(tradingDay, symbol, effectiveTime, expireTime, price, side)
            return twap

        def cal_twap_slipage(twap, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByTwap = 0.00 if twap == 0.00 else ((avgprice - twap) / twap if side == 'Sell' else (twap - avgprice) / twap)
            return slipageByTwap

        for tradingDay in tradingDays:
            for clientId in clientIds:
                self.logger.info(f'start calculator: {tradingDay}__{clientId}')
                self.email.add_email_content(f'{tradingDay}_({clientId})交易报告，请查收')

                clientOrders = self.get_clientOrder(tradingDay, clientId)
                clientOrders.sort_values(by=['effectiveTime'], inplace=True)
                if clientOrders.size > 0:
                    # 1.计算twap
                    clientOrders['twap'] = clientOrders.apply(
                        lambda x: cal_twap(tradingDay, x['effectiveTime'], x['expireTime'], x['symbol'], x['price'],
                                           x['side']), axis=1)

                    # 2.计算slipageByTwap
                    clientOrders['slipageByTwap'] = clientOrders.apply(
                        lambda x: cal_twap_slipage(x['twap'], x['side'], x['avgprice']), axis=1)
                    clientOrders.loc[clientOrders.loc[:, 'cumQty'] == 0, ['slipageByTwap']] = 0.00
                    clientOrders['twap'] = round(clientOrders['twap'], 5)
                    clientOrders['slipageByTwap'] = round(clientOrders['slipageByTwap'] * 10000, 2)

                    # 3.cancel_ratio 计算撤单率
                    df_cancel_ratio = self.cal_cancel_ratio(tradingDay, clientId)
                    cancel_ratio = sum(df_cancel_ratio[df_cancel_ratio['orderStatus'] == 'Canceled']['Qty']) / sum(
                        df_cancel_ratio[df_cancel_ratio['orderStatus'] != 'Rejected']['Qty'])
                    cancel_ratio = round(np.float64(cancel_ratio) * 100, 2)

                    # 4.计算成交额
                    turnover = sum(clientOrders['turnover'])

                    # 5.计算VwapBySlipage
                    slipage_by_vwap = sum(clientOrders['turnover'] * clientOrders['slipageByVwap']) / sum(
                        clientOrders['turnover'])
                    # 6.计算TwapBySlipage
                    slipage_by_twap = sum(clientOrders['turnover'] * clientOrders['slipageByTwap']) / sum(
                        clientOrders['turnover'])

                    df_summary = pd.DataFrame(
                        {'cancelRatio': [cancel_ratio], 'turnover': [turnover], "slipage_by_vwap": [slipage_by_vwap],
                         'slipage_by_twap': [slipage_by_twap]})

                    df_receive = self.get_receiveList(clientId)
                    df_receive['tradingDay'] = tradingDay
                    # if df_receive.shape[0] > 0:
                    #     receivers = df_receive.loc[0, 'repsentEmail'].split(';')

                    fileName = f'{tradingDay}_({clientId}).xlsx'
                    pathCsv = os.path.join(f'Data/{fileName}')

                    ExcelHelper.createExcel(pathCsv)
                    ExcelHelper.Append_df_to_excel(pathCsv, clientOrders,
                                                   header=True, sheet_name=clientId)
                    ExcelHelper.Append_df_to_excel(pathCsv, df_summary, header=True,
                                                   interval=4, sheet_name=clientId)
                    ExcelHelper.removeSheet(pathCsv, 'Sheet')
                    self.email.send_email_file(pathCsv, fileName, df_receive)
                    self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')


if __name__ == '__main__':
    cfg = RawConfigParser()
    cfg.read('config.ini')
    clientIds = cfg.get('AlgoTradeReport', 'id')
    start = sys.argv[1]
    end = sys.argv[2]
    reporter = AlgoTradeReporter(start, end, clientIds)
