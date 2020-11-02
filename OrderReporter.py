import pymssql
import pandas as pd
import h5py
import sys
import Log
import os
import numpy as np
from configparser import RawConfigParser
import datetime

from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader
from DataSender.EmailHelper import EmailHelper

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class OrderReporter(object):
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
        to_receiver = []
        cc_receiver = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from Clients where clientId = \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    accountIds.append(row['accountId'])
                    clientIds.append(row['clientId'])
                    clientName.append(row['clientName'])
                    to_receiver.append(row['email'])
                    cc_receiver.append(row['repsentEmail'])

        data = pd.DataFrame(
            {'accountId': accountIds, 'clientId': clientIds, 'clientName': clientName, 'to_receiver': to_receiver,
             'cc_receiver': cc_receiver})
        return data

    def get_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        orderId = []
        symbol = []
        side = []
        effectiveTime = []
        expireTime = []
        avgprice = []
        cumQty = []
        slipageByVwap = []
        algo = []
        orderQty = []
        orderStatus = []
        VWAP = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId like \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    orderId.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    avgprice.append(row['avgPrice'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    algo.append(row['algo'])
                    orderQty.append(row['orderQty'])
                    orderStatus.append(row['orderStatus'])
                    VWAP.append(row['iVWP'])

        data = pd.DataFrame({'orderId': orderId, 'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime,
                             'expireTime': expireTime, 'avgPrice': avgprice, 'orderQty': orderQty, 'cumQty': cumQty,
                             'algo': algo, 'orderStatus': orderStatus, 'VWAP': VWAP, 'slipageByVWAP': slipageByVwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgPrice'] = data['avgPrice'].astype('float')
        data['slipageByVWAP'] = data['slipageByVWAP'].astype('float')
        data['VWAP'] = data['VWAP'].astype('float')
        data['turnover'] = data['avgPrice'] * data['cumQty']
        return data

    def get_client_order_count(self, tradingday, clientId):
        orderId = []
        sliceStatus = []
        sliceCount = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT a.orderId, sliceStatus, sliceCount FROM ClientOrderView a JOIN (SELECT orderId,orderStatus AS sliceStatus,COUNT (*) AS sliceCount FROM ExchangeOrderView WHERE orderStatus IN ('Filled', 'Canceled') GROUP BY orderId,orderStatus) b ON a.orderId = b.orderId WHERE a.tradingDay = \'{tradingday}\' AND a.clientId like \'{clientId}\' ORDER BY a.orderId"
                cursor.execute(stmt)
                for row in cursor:
                    orderId.append(row['orderId'])
                    sliceStatus.append(row['sliceStatus'])
                    sliceCount.append(row['sliceCount'])

        data = pd.DataFrame({'orderId': orderId, 'sliceStatus': sliceStatus, 'sliceCount': sliceCount})
        data['sliceCount'] = data['sliceCount'].astype('int')
        return data

    def get_exchangeOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        sliceId = []
        orderId = []
        side = []
        symbol = []
        effectiveTime = []
        qty = []
        cumQty = []
        leavesQty = []
        price = []
        sliceAvgPrice = []
        orderStatus = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.clientId like \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    sliceId.append(row['sliceId'])
                    orderId.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    qty.append(row['qty'])
                    leavesQty.append(row['leavesQty'])
                    cumQty.append(row['cumQty'])
                    sliceAvgPrice.append(row['sliceAvgPrice'])
                    price.append(row['price'])
                    orderStatus.append(row['orderStatus'])

        data = pd.DataFrame(
            {'sliceId': sliceId, 'orderId': orderId, 'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime,
             'qty': qty, 'cumQty': cumQty, 'leavesQty': leavesQty, 'price': price, 'sliceAvgPrice': sliceAvgPrice,
             'orderStatus': orderStatus})

        data['cumQty'] = data['cumQty'].astype('int')
        if data['effectiveTime'].shape[0] > 0:
            data['effectiveTime'] = (data['effectiveTime'] + datetime.timedelta(hours=8)).map(
                lambda x: x.strftime('%H:%M:%S'))
            data.sort_values(by=['effectiveTime'], inplace=True)
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

    def get_tick_by_symbol(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        df_tick_symbol = self.read_symbol_tick(tradingDay, symbol)
        if df_tick_symbol.shape[0] == 0:
            return pd.DataFrame()
        if price == 0:
            return df_tick_symbol[(df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                    df_tick_symbol['Volume'] > 0)]
        else:
            if side == 'Buy' or side == 1:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0)]
            else:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0)]

    def get_twap(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        data = self.get_tick_by_symbol(tradingDay, symbol, startTime, endTime, price, side)
        return data.Price.sum() / data.Volume.count() if data.size > 0 else 0

    def stat_summary(self, df, side, field):
        df = df[(df['side'] == side) & (df[field] != 0)]
        amt = sum(df['turnover'])
        if side == 'Buy':
            slipage = 0 if amt == 0 else sum((df[field] - df['avgPrice']) / df[field] * df['turnover']) / sum(
                df['turnover'])
        else:
            slipage = 0 if amt == 0 else sum((df['avgPrice'] - df[field]) / df[field] * df['turnover']) / sum(
                df['turnover'])
        pnl_yuan = slipage * amt
        return amt, slipage, pnl_yuan

    def run(self, tradingDays, clientIds):
        def cal_twap(tradingDay, effectiveTime, expireTime, symbol, price, side, cumQty):
            if cumQty == 0:
                return 0
            effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol}-{price}-{side}')
            twap = self.get_twap(tradingDay, symbol, effectiveTime, expireTime, price, side)
            return twap

        def cal_twap_slipage(twap, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByTwap = 0.00 if twap == 0.00 else (
                (avgprice - twap) / twap if side == 'Sell' else (twap - avgprice) / twap)
            return slipageByTwap

        def cal_ocp(tradingDay, expireTime, symbol, cumQty):
            if cumQty == 0: return 0
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_ocp-{tradingDay}-{expireTime}-{symbol}')
            tick = read_tick(symbol, tradingDay)
            tick = tick[tick['Time'] <= expireTime]
            return tick.tail(1).iloc[0, :]['Price']

        def cal_ocp_slipage(ocp, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByOCP = 0.00 if ocp == 0.00 else (
                (avgprice - ocp) / ocp if side == 'Sell' else (ocp - avgprice) / ocp)
            return slipageByOCP

        def read_tick(symbol, tradingday):
            """
            read tick data
            :param symbol: '600000.sh' str
            :param tradingday: '20170104' str
            :return: pd.DataFrame Time类型：93003000 int
            """
            with h5py.File(os.path.join(self.tick_path, ''.join([tradingday, '.h5'])), 'r') as f:
                if symbol not in f.keys():
                    raise Exception(f'{tradingday}_{symbol} tick 为空')
                time = f[symbol]['Time']
                if len(time) == 0:
                    raise Exception(f'{tradingday}_{symbol} tick 为空')
                price = f[symbol]['Price']
                volume = f[symbol]['Volume']
                turnover = f[symbol]['Turnover']
                tick = pd.DataFrame(
                    {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover})

            return tick

        for tradingDay in tradingDays:
            if not os.path.exists(os.path.join(self.tick_path, tradingDay + '.h5')):
                self.logger.error(f'{tradingDay} h5 tick is not existed.')
                continue
            for clientId in clientIds:
                self.logger.info(f'start calculator: {tradingDay}__{clientId}')
                clientOrders = self.get_clientOrder(tradingDay, clientId)
                if clientOrders.size == 0:
                    continue
                df_client_order_count = self.get_client_order_count(tradingDay, clientId)
                df_client_order_count['sliceStatus'] = df_client_order_count['sliceStatus'].map(lambda x: x + 'Count')
                df_client_order_count = df_client_order_count.pivot(index='orderId', columns='sliceStatus',
                                                                    values='sliceCount')
                clientOrders = clientOrders.merge(df_client_order_count, how='left', left_on='orderId',
                                                  right_index=True)
                clientOrders.fillna(0, inplace=True)
                clientOrders.sort_values(by=['effectiveTime'], inplace=True)

                # 调整列顺序
                VWAPs = clientOrders.pop('VWAP')
                clientOrders.insert(clientOrders.shape[1], 'VWAP', VWAPs)
                slipageByVWAPs = clientOrders.pop('slipageByVWAP')
                clientOrders.insert(clientOrders.shape[1], 'slipageByVWAP', slipageByVWAPs)

                # 1.计算twap
                clientOrders['TWAP'] = clientOrders.apply(
                    lambda x: cal_twap(tradingDay, x['effectiveTime'], x['expireTime'], x['symbol'], x['avgPrice'],
                                       x['side'], x['cumQty']), axis=1)

                # 2.计算slipageByTwap
                clientOrders['slipageByTWAP'] = clientOrders.apply(
                    lambda x: cal_twap_slipage(x['TWAP'], x['side'], x['avgPrice']), axis=1)
                clientOrders.loc[clientOrders.loc[:, 'cumQty'] == 0, ['slipageByTWAP']] = 0.00
                clientOrders['TWAP'] = round(clientOrders['TWAP'], 5)
                clientOrders['slipageByTWAP'] = round(clientOrders['slipageByTWAP'] * 10000, 2)

                # 3.计算OrderClosePx
                clientOrders['OCP'] = clientOrders.apply(
                    lambda x: cal_ocp(tradingDay, x['expireTime'], x['symbol'], x['cumQty']), axis=1)
                clientOrders['slipageByOCP'] = clientOrders.apply(
                    lambda x: cal_ocp_slipage(x['OCP'], x['side'], x['avgPrice']), axis=1)
                clientOrders['slipageByOCP'] = round(clientOrders['slipageByOCP'] * 10000, 2)

                df_exchange_order = self.get_exchangeOrder(tradingday=tradingDay, clientId=clientId)

                buy_amt, buy_vwap_slipage, buy_pnl_vwap_yuan = self.stat_summary(clientOrders, 'Buy', 'VWAP')
                buy_amt, buy_twap_slipage, buy_pnl_twap_yuan = self.stat_summary(clientOrders, 'Buy', 'TWAP')
                buy_amt, buy_ocp_slipage, buy_pnl_ocp_yuan = self.stat_summary(clientOrders, 'Buy', 'OCP')

                sell_amt, sell_vwap_slipage, sell_pnl_vwap_yuan = self.stat_summary(clientOrders, 'Sell', 'VWAP')
                sell_amt, sell_twap_slipage, sell_pnl_twap_yuan = self.stat_summary(clientOrders, 'Sell', 'TWAP')
                sell_amt, sell_ocp_slipage, sell_pnl_ocp_yuan = self.stat_summary(clientOrders, 'Sell', 'OCP')

                df_summary = pd.DataFrame(
                    {'Side': ['Buy', 'Sell'], 'FilledAmt(万元)': [round(buy_amt / 10000, 3), round(sell_amt / 10000, 3)],
                     'PnL2VWAP(BPS)': [round(buy_vwap_slipage * 10000, 2),
                                       round(sell_vwap_slipage * 10000, 2)],
                     'PnL2TWAP(BPS)': [round(buy_twap_slipage * 10000, 2),
                                       round(sell_twap_slipage * 10000, 2)],
                     'PnL2OCP(BPS)': [round(buy_ocp_slipage * 10000, 2),
                                      round(sell_ocp_slipage * 10000, 2)],
                     'PnL2VWAP(YUAN)': [round(buy_pnl_vwap_yuan, 2),
                                        round(sell_pnl_vwap_yuan, 2)],
                     'PnL2TWAP(YUAN)': [round(buy_pnl_twap_yuan, 2),
                                        round(sell_pnl_twap_yuan, 2)],
                     'PnL2OCP(YUAN)': [round(buy_pnl_ocp_yuan, 2),
                                       round(sell_pnl_ocp_yuan, 2)]
                     }, index=[1, 2])

                df_receive = self.get_receiveList(clientId)
                df_receive['tradingDay'] = tradingDay

                self.email.add_email_content(f'ClientOrderReporter_{tradingDay}_({clientId})交易报告，请查收')
                fileName = f'OrderReporter_{tradingDay}_({clientId}).xlsx'
                pathCsv = os.path.join(f'Data/{fileName}')

                ExcelHelper.createExcel(pathCsv)
                clientOrders['effectiveTime'] = clientOrders['effectiveTime'].map(lambda x: x.strftime('%H:%M:%S'))
                clientOrders['expireTime'] = clientOrders['expireTime'].map(lambda x: x.strftime('%H:%M:%S'))
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_summary, header=True,
                                               sheet_name='algoSummary', sep_key='all_name')
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=clientOrders,
                                               header=True, sheet_name='algoClientOrder', sep_key='all_name')
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_exchange_order,
                                               header=True, sheet_name='algoExchangeOrder', sep_key='all_name')
                ExcelHelper.removeSheet(pathCsv, 'Sheet')

                self.email.send_email_file(pathCsv, fileName, df_receive, subject_prefix='OrderReporter')
                self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')


if __name__ == '__main__':
    cfg = RawConfigParser()
    cfg.read('config.ini', encoding='utf-8')
    clientIds = cfg.get('AlgoTradeReport', 'id')
    start = sys.argv[1]
    end = sys.argv[2]
    reporter = OrderReporter(start, end, clientIds)
