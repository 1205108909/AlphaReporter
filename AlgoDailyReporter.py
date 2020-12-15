#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : AlgoDailyReporter.py 
@Time : 2020/12/14 17:31 
"""

import os
import sys
from configparser import RawConfigParser

import h5py
import numpy as np
import pandas as pd
import pymssql
from sqlalchemy import create_engine
import datetime
import Log
from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class AlgoDailyReporter(object):
    def __init__(self, tradingDay):
        self.logger = Log.get_logger(__name__)
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.port = 1433
        self.tick_path = "Y:/Data/h5data/stock/tick/"
        self.lastUpdId = 'wzy'
        self.lastUpdDt = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(tradingDay, tradingDay)

        cfg = RawConfigParser()
        cfg.read('config.ini', encoding='utf-8')
        clientIds = cfg.get('AlgoDailyReport', 'id')
        clientIDs = list(clientIds.split(';'))
        self.run(tradingdays, clientIDs)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                print(e)

    def get_all_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        clientIds = []
        accountIds = []
        orderIds = []
        symbol = []
        effectiveTime = []
        expireTime = []
        exDestination = []
        side = []
        cumQty = []
        slipageByVwap = []
        avgprice = []
        ivwap = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from ClientOrder where orderid in(SELECT distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId = \'{clientId}\' and tradingDay=\'{tradingday}\')) order by slipageInBps asc, cumqty desc"
                cursor.execute(stmt)
                for row in cursor:
                    clientIds.append(row['clientId'])
                    accountIds.append(row['accountId'])
                    orderIds.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame(
            {'clientId': clientIds, 'accountId': accountIds, 'orderId': orderIds, 'symbol': symbol, 'side': side,
             'effectiveTime': effectiveTime, 'expireTime': expireTime,
             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
             'slipageByVwap': slipageByVwap, 'ivwap': ivwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgprice'] = data['avgprice'].astype('float')
        data['slipageByVwap'] = data['slipageByVwap'].astype('float')
        data['turnover'] = data['avgprice'] * data['cumQty']
        return data

    def get_nothas_signal_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        clientIds = []
        accountIds = []
        orderIds = []
        symbol = []
        effectiveTime = []
        expireTime = []
        exDestination = []
        side = []
        cumQty = []
        slipageByVwap = []
        avgprice = []
        ivwap = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                # stmt = f"select * from ClientOrder where orderid in(SELECT distinct(orderid) FROM ExchangeOrder where orderid not in(SELECT orderid FROM ClientOrder where clientId = \'{clientId}\' and tradingDay=\'{tradingday}\')and  signalside>0) order by slipageInBps asc, cumqty desc"
                stmt = f"   select * from ClientOrder where  clientId = \'{clientId}\' and tradingDay=\'{tradingday}\' and orderid not in(SELECT  distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId = \'{clientId}\' and tradingDay=\'{tradingday}\') and  signalside>0)  order by slipageInBps asc"
                cursor.execute(stmt)
                for row in cursor:
                    clientIds.append(row['clientId'])
                    accountIds.append(row['accountId'])
                    orderIds.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame(
            {'clientId': clientIds, 'accountId': accountIds, 'orderId': orderIds, 'symbol': symbol, 'side': side,
             'effectiveTime': effectiveTime, 'expireTime': expireTime,
             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
             'slipageByVwap': slipageByVwap, 'ivwap': ivwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgprice'] = data['avgprice'].astype('float')
        data['slipageByVwap'] = data['slipageByVwap'].astype('float')
        data['turnover'] = data['avgprice'] * data['cumQty']
        return data

    def get_has_signal_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        clientIds = []
        accountIds = []
        orderIds = []
        symbol = []
        effectiveTime = []
        expireTime = []
        exDestination = []
        side = []
        cumQty = []
        slipageByVwap = []
        avgprice = []
        ivwap = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from ClientOrder where orderid in(SELECT distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId = \'{clientId}\' and tradingDay=\'{tradingday}\')and  signalside>0) order by slipageInBps asc, cumqty desc"
                cursor.execute(stmt)
                for row in cursor:
                    clientIds.append(row['clientId'])
                    accountIds.append(row['accountId'])
                    orderIds.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame(
            {'clientId': clientIds, 'accountId': accountIds, 'orderId': orderIds, 'symbol': symbol, 'side': side,
             'effectiveTime': effectiveTime, 'expireTime': expireTime,
             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
             'slipageByVwap': slipageByVwap, 'ivwap': ivwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgprice'] = data['avgprice'].astype('float')
        data['slipageByVwap'] = data['slipageByVwap'].astype('float')
        data['turnover'] = data['avgprice'] * data['cumQty']
        return data

    def join_client_exchange_Order(self, tradingDay, clientId):
        accountIds = []
        symbol = []
        effectiveTime = []
        expireTime = []
        exDestination = []
        side = []
        cumQty = []
        slipageByVwap = []
        avgprice = []
        signalType = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT a.accountId,a.orderId,	a.symbol,	a.tradingDay,	a.side,	b.alphaPrice,	b.cumQty,	a.exDestination AS exDestination,	a.effectiveTime,	a.expireTime,	a.avgPrice,	(CASE	WHEN a.side = 1 THEN (a.iVWP - b.alphaPrice) / a.iVWP	ELSE (b.alphaPrice - a.iVWP) / a.iVWP	END) AS delta,b.signalType FROM	(SELECT accountId, orderId, tradingDay, side, avgPrice, symbol, exDestination, effectiveTime,expireTime,iVWP	FROM ClientOrder WHERE	clientId = \'{clientId}\') a JOIN (SELECT	orderId,	SUM (cumQty * sliceAvgPrice) / (CASE WHEN SUM (cumQty) > 0 THEN	SUM (cumQty) ELSE	NULL END) AS alphaPrice,SUM (cumQty) AS cumQty,	CASE WHEN (TEXT LIKE '%Sell%LongHolding%' OR TEXT LIKE '%Buy%ShortHolding%'	) THEN 'Reverse' WHEN ( TEXT LIKE '%Buy%LongHolding%' OR TEXT LIKE '%Sell%ShortHolding%' ) THEN 'Forward'	WHEN (TEXT LIKE '%ToNormal%') THEN		'Close'	WHEN decisionType = '22' THEN		'First1'	ELSE		'Normal'	END AS signalType	FROM ExchangeOrder	WHERE		cumQty > 0	GROUP BY		orderId,		CASE	WHEN (		TEXT LIKE '%Sell%LongHolding%'		OR TEXT LIKE '%Buy%ShortHolding%'	) THEN		'Reverse'	WHEN (		TEXT LIKE '%Buy%LongHolding%'	OR TEXT LIKE '%Sell%ShortHolding%') THEN 'Forward' WHEN (TEXT LIKE '%ToNormal%') THEN	'Close'	WHEN decisionType = '22' THEN	'First1' ELSE	'Normal'	END) b ON a.orderId = b.orderId AND a.tradingDay = \'{tradingDay}\'"
                cursor.execute(stmt)
                for row in cursor:
                    accountIds.append(row['accountId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    avgprice.append(row['avgPrice'])
                    slipageByVwap.append(row['delta'])
                    signalType.append(row['signalType'])

        data = pd.DataFrame({'accountId': accountIds, 'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime,
                             'expireTime': expireTime,
                             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
                             'slipageByVwap': slipageByVwap, 'signalType': signalType})

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

    def run(self, tradingDays, clientIds):
        def get_twap(tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
            data = self.get_tick_by_symbol(tradingDay, symbol, startTime, endTime, price, side)
            return data.Price.sum() / data.Volume.count() if data.size > 0 else 0

        def cal_twap(tradingDay, effectiveTime, expireTime, symbol, price, side, cumQty):
            if cumQty == 0:
                return 0
            effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol}-{price}-{side}')
            twap = get_twap(tradingDay, symbol, effectiveTime, expireTime, price, side)
            return twap

        def cal_twap_slipage(twap, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByTwap = 0.00 if twap == 0.00 else (
                (avgprice - twap) / twap if side == 'Sell' else (twap - avgprice) / twap)
            return slipageByTwap

        for tradingDay in tradingDays:
            for clientId in clientIds:
                self.logger.info(f'start calculator: {tradingDay}__{clientId}')
                # 1.所有订单
                all_clientOrders = self.get_all_clientOrder(tradingDay, clientId)
                all_clientOrders['TWAP'] = all_clientOrders.apply(
                    lambda x: cal_twap(tradingDay, x['effectiveTime'], x['expireTime'], x['symbol'], x['avgprice'],
                                       x['side'], x['cumQty']), axis=1)

                # 2.计算slipageByTwap
                all_clientOrders['slipageByTwap'] = all_clientOrders.apply(
                    lambda x: cal_twap_slipage(x['TWAP'], x['side'], x['avgprice']), axis=1)

                # 3.所有信号单
                all_has_signal_clientOrders = self.get_has_signal_clientOrder(tradingDay, clientId)  # 所有信号单
                all_clientOrders['flag'] = all_clientOrders.apply(
                    lambda x: x['orderId'] in all_has_signal_clientOrders['orderId'].tolist(), axis=1)
                all_has_signal_clientOrders = all_clientOrders[all_clientOrders['flag']]
                all_has_signal_clientOrders.reset_index(inplace=True)

                # 4.所有非信号单
                all_nothas_signal_clientOrders = self.get_nothas_signal_clientOrder(tradingDay, clientId)  # 所有非信号单
                all_clientOrders['flag'] = all_clientOrders.apply(
                    lambda x: x['orderId'] in all_nothas_signal_clientOrders['orderId'].tolist(), axis=1)
                all_nothas_signal_clientOrders = all_clientOrders[all_clientOrders['flag']]
                all_nothas_signal_clientOrders.reset_index(inplace=True)

                # 5.订单分组计算VWAPBps,TWAPBps
                all_clientOrders_group = all_clientOrders.groupby(['accountId', 'exDestination', 'side'])
                series_size = all_clientOrders_group.size()
                series_size.rename('count', inplace=True)
                series_turnover = all_clientOrders_group['turnover'].sum()
                series_turnover.rename('turnover', inplace=True)
                series_vwapbps = all_clientOrders_group.apply(
                    lambda x: sum(x['turnover'] * x['slipageByVwap']) / sum(x['turnover']))
                series_vwapbps.rename('VWAPBps', inplace=True)
                series_twapbps = all_clientOrders_group.apply(
                    lambda x: sum(x['turnover'] * x['slipageByTwap']) / sum(x['turnover']))
                series_twapbps.rename('TWAPBps', inplace=True)

                # 6.信号单分组计算VWAPBps,TWAPBps
                series_signal_turnover, series_signal_vwapbps, series_signal_twapbps = self.get_turnover_vwap_twap(
                    all_has_signal_clientOrders)
                series_signal_turnover.rename('signalTurnover', inplace=True)
                series_signal_vwapbps.rename('signalVWAPBps', inplace=True)
                series_signal_twapbps.rename('signalTWAPBps', inplace=True)

                # 7.非信号单分组计算VWAPBps,TWAPBps
                series_not_signal_turnover, series_not_signal_vwapbps, series_not_signal_twapbps = self.get_turnover_vwap_twap(
                    all_nothas_signal_clientOrders)
                series_not_signal_turnover.rename('notSignalTurnover', inplace=True)
                series_not_signal_vwapbps.rename('notSignalVWAPBps', inplace=True)
                series_not_signal_twapbps.rename('notSignalTWAPBps', inplace=True)

                # 8.同向反向首次信号计算VWAPBps,TWAPBps
                df_client_exchange_order = self.join_client_exchange_Order(tradingDay=tradingDay, clientId=clientId)
                df_client_exchange_order['TWAP'] = df_client_exchange_order.apply(
                    lambda x: cal_twap(tradingDay, x['effectiveTime'], x['expireTime'], x['symbol'], x['avgprice'],
                                       x['side'], x['cumQty']), axis=1)
                df_client_exchange_order['slipageByTwap'] = df_client_exchange_order.apply(
                    lambda x: cal_twap_slipage(x['TWAP'], x['side'], x['avgprice']), axis=1)

                # 9.同向信号订单
                df_client_exchange_order_forward = df_client_exchange_order[
                    df_client_exchange_order['signalType'] == 'Forward']
                series_forward_turnover, series_forward_vwapbps, series_forward_twapbps = self.get_turnover_vwap_twap(
                    df_client_exchange_order_forward)
                series_forward_turnover.rename('forwardTurnover', inplace=True)
                series_forward_vwapbps.rename('forwardVWAPBps', inplace=True)
                series_forward_twapbps.rename('forwardTWAPBps', inplace=True)

                # 10.反向信号订单
                df_client_exchange_order_reverse = df_client_exchange_order[
                    df_client_exchange_order['signalType'] == 'Reverse']
                series_reverse_turnover, series_reverse_vwapbps, series_reverse_twapbps = self.get_turnover_vwap_twap(
                    df_client_exchange_order_reverse)
                series_reverse_turnover.rename('reverseTurnover', inplace=True)
                series_reverse_vwapbps.rename('reverseVWAPBps', inplace=True)
                series_reverse_twapbps.rename('reverseTWAPBps', inplace=True)

                # 11.首次信号订单
                df_client_exchange_order_first1 = df_client_exchange_order[
                    df_client_exchange_order['signalType'] == 'First1']
                series_first_turnover, series_first_vwapbps, series_first_twapbps = self.get_turnover_vwap_twap(
                    df_client_exchange_order_first1)
                series_first_turnover.rename('firstTurnover', inplace=True)
                series_first_vwapbps.rename('firstVWAPBps', inplace=True)
                series_first_twapbps.rename('firstTWAPBps', inplace=True)

                df_result = pd.concat(
                    [series_size, series_turnover, series_vwapbps, series_twapbps, series_signal_turnover,
                     series_signal_vwapbps, series_signal_twapbps, series_not_signal_turnover,
                     series_not_signal_vwapbps, series_not_signal_twapbps, series_forward_vwapbps,
                     series_forward_twapbps, series_reverse_vwapbps, series_reverse_twapbps, series_first_vwapbps,
                     series_first_twapbps], axis=1)
                df_result.reset_index(inplace=True)
                df_result.fillna(0, inplace=True)
                df_result['tradingDay'] = tradingDay
                df_result['clientId'] = clientId
                df_result['lastUpdId'] = self.lastUpdId
                df_result['lastUpdDt'] = self.lastUpdDt

                list_index = ['tradingDay', 'clientId', 'accountId', 'exDestination', 'side', 'count', 'turnover',
                              'VWAPBps', 'TWAPBps', 'signalTurnover', 'signalVWAPBps', 'signalTWAPBps',
                              'notSignalTurnover', 'notSignalVWAPBps', 'notSignalTWAPBps', 'forwardVWAPBps',
                              'forwardTWAPBps', 'reverseVWAPBps', 'reverseTWAPBps', 'firstVWAPBps', 'firstTWAPBps',
                              'lastUpdId', 'lastUpdDt']
                df_result = df_result[list_index]
                self.Insert_Table(df_result, 'AlgoDailyReport')
                self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')

    def get_turnover_vwap_twap(self, df):
        if df.shape[0] == 0:
            return pd.Series(), pd.Series(), pd.Series()
        else:
            df_group = df.groupby(['accountId', 'exDestination', 'side'])
            series_turnover = df_group['turnover'].sum()
            series_vwapbps = df_group.apply(lambda x: sum(x['turnover'] * x['slipageByVwap']) / sum(x['turnover']))
            series_twapbps = df_group.apply(lambda x: sum(x['turnover'] * x['slipageByTwap']) / sum(x['turnover']))
            return series_turnover, series_vwapbps, series_twapbps

    def Insert_Table(self, df, tablename):
        """
        :param1 df:要写入db的DataFrame
        :param2 tablename:要写入的Tablename
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    for index, row in df.iterrows():
                        sql = f'DELETE FROM {tablename} WHERE tradingDay = \'{row["tradingDay"]}\' AND clientID = \'{row["clientId"]}\' AND accountId = \'{row["accountId"]}\' AND exDestination = \'{row["exDestination"]}\' AND side = \'{row["side"]}\''
                        self.logger.info('execute sql : ' + sql)
                        cursor.execute(sql)
                conn.commit()  # 提交之前的操作，如果之前已经执行多次的execute，那么就都进行提交

            engine = create_engine(
                f'mssql+pymssql://{self.user}:{self.password}@{self.server}:{self.port}/{self.database}')
            df.to_sql(tablename, engine, if_exists='append', index=False)

        except Exception as e:
            self.logger.error(e)
            self.logger.error(f"Insert {tablename} Fail")


if __name__ == '__main__':
    tradingDay = sys.argv[1]
    reporter = AlgoDailyReporter(tradingDay)
