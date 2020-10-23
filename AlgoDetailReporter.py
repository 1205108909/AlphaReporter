#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : AlgoDetailReporter.py 
@Time : 2020/10/22 20:02 
"""

import os
import sys
from configparser import RawConfigParser

import h5py
import pandas as pd
import pymssql

import Log
from DataSender.EmailHelper import EmailHelper
from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class AlgoDetailReporter(object):
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

    def get_all_clientOrder(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
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
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame({'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime, 'expireTime': expireTime,
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
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame({'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime, 'expireTime': expireTime,
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
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    avgprice.append(row['avgPrice'])
                    ivwap.append(row['iVWP'])

        data = pd.DataFrame({'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime, 'expireTime': expireTime,
                             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
                             'slipageByVwap': slipageByVwap, 'ivwap': ivwap})

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

    def join_client_exchange_Order(self, tradingDay, clientId):
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
                stmt = f"SELECT	a.orderId,	a.symbol,	a.tradingDay,	a.side,	b.alphaPrice,	b.cumQty,	a.exDestination AS exDestination,	a.effectiveTime,	a.expireTime,	a.avgPrice,	(CASE	WHEN a.side = 1 THEN (a.iVWP - b.alphaPrice) / a.iVWP	ELSE (b.alphaPrice - a.iVWP) / a.iVWP	END) AS delta,b.signalType FROM	(SELECT orderId, tradingDay, side, avgPrice, symbol, exDestination, effectiveTime,expireTime,iVWP	FROM ClientOrder WHERE	clientId = \'{clientId}\') a JOIN (SELECT	orderId,	SUM (cumQty * sliceAvgPrice) / (CASE WHEN SUM (cumQty) > 0 THEN	SUM (cumQty) ELSE	NULL END) AS alphaPrice,SUM (cumQty) AS cumQty,	CASE WHEN (TEXT LIKE '%Sell%LongHolding%' OR TEXT LIKE '%Buy%ShortHolding%'	) THEN 'Reverse' WHEN ( TEXT LIKE '%Buy%LongHolding%' OR TEXT LIKE '%Sell%ShortHolding%' ) THEN 'Forward'	WHEN (TEXT LIKE '%ToNormal%') THEN		'Close'	WHEN decisionType = '22' THEN		'First1'	ELSE		'Normal'	END AS signalType	FROM ExchangeOrder	WHERE		cumQty > 0	GROUP BY		orderId,		CASE	WHEN (		TEXT LIKE '%Sell%LongHolding%'		OR TEXT LIKE '%Buy%ShortHolding%'	) THEN		'Reverse'	WHEN (		TEXT LIKE '%Buy%LongHolding%'	OR TEXT LIKE '%Sell%ShortHolding%') THEN 'Forward' WHEN (TEXT LIKE '%ToNormal%') THEN	'Close'	WHEN decisionType = '22' THEN	'First1' ELSE	'Normal'	END) b ON a.orderId = b.orderId AND a.tradingDay = \'{tradingDay}\'"
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    exDestination.append(row['exDestination'])
                    cumQty.append(row['cumQty'])
                    avgprice.append(row['avgPrice'])
                    slipageByVwap.append(row['delta'])
                    signalType.append(row['signalType'])

        data = pd.DataFrame({'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime, 'expireTime': expireTime,
                             'exDestination': exDestination, 'avgprice': avgprice, 'cumQty': cumQty,
                             'slipageByVwap': slipageByVwap, 'signalType': signalType})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgprice'] = data['avgprice'].astype('float')
        data['slipageByVwap'] = data['slipageByVwap'].astype('float')
        data['turnover'] = data['avgprice'] * data['cumQty']
        return data

    def stat_effect_by_signal(self, tradingDay, clientId):
        signalType = []
        expireTime = []
        amount = []
        slipage = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT signalType AS signalType, expireTime AS expireTime, SUM (c.cumQty * c.alphaPrice) AS amount, SUM(delta * c.cumQty * c.alphaPrice ) / SUM(c.cumQty * c.alphaPrice) AS slipage FROM(SELECT a.orderId,a.symbol,a.expireTime,a.tradingDay,a.side,b.alphaPrice,b.cumQty,a.avgPrice,a.cumQty AS clientQty,(CASE	WHEN a.side = 1 THEN (a.iVWP - b.alphaPrice) / a.iVWP	ELSE (b.alphaPrice - a.iVWP) / a.iVWP	END) AS delta,b.signalType FROM(SELECT orderId,tradingDay,side,avgPrice, symbol,iVWP,cumQty,expireTime FROM ClientOrder	WHERE	clientId LIKE \'{clientId}\') a	JOIN (SELECT orderId,SUM (cumQty * sliceAvgPrice)/(CASE WHEN SUM (cumQty) > 0 THEN	SUM (cumQty)	ELSE	NULL	END) AS alphaPrice,	SUM (cumQty) AS cumQty,	CASE WHEN (signalSide > 0) THEN	'Signal' ELSE 'Normal' END AS signalType FROM	ExchangeOrder WHERE cumQty>0 GROUP BY orderId,CASE WHEN (signalSide > 0) THEN	'Signal' ELSE 'Normal' END,expireTime) b ON a.orderId = b.orderId AND a.tradingDay = \'{tradingDay}\') c GROUP BY signalType,expireTime"
                cursor.execute(stmt)
                for row in cursor:
                    signalType.append(row['signalType'])
                    expireTime.append(row['expireTime'])
                    amount.append(row['amount'])
                    slipage.append(row['slipage'])

        data = pd.DataFrame({'expireTime': expireTime, 'signalType': signalType, 'amount': amount, 'slipage': slipage})
        data['expireTime'] = data['expireTime'].map(lambda x: x.strftime('%H:%M:%S'))
        data['amount'] = data['amount'].astype('float')
        data['amount'] = round(data['amount'] / 10000, 2)
        data['slipage'] = data['slipage'].astype('float')
        data['slipage'] = round(data['slipage'] * 10000, 2)
        return data

    def stat_effect_by_side(self, tradingDay, clientId):
        side = []
        expireTime = []
        amount = []
        slipage = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT c.side AS side,expireTime AS expireTime,SUM (c.cumQty * c.alphaPrice) AS amount,	SUM (delta * c.cumQty * c.alphaPrice) / SUM (c.cumQty * c.alphaPrice) AS slipage FROM	(SELECT	a.orderId,a.symbol,a.expireTime,a.tradingDay,	a.side,b.alphaPrice,b.cumQty,a.avgPrice,	a.cumQty AS clientQty,(CASE	WHEN a.side = 1 THEN (a.iVWP - b.alphaPrice) / a.iVWP	ELSE(b.alphaPrice - a.iVWP) / a.iVWP END) AS delta FROM	(SELECT	orderId,tradingDay,side,avgPrice,symbol,iVWP,cumQty,expireTime FROM	ClientOrder	WHERE clientId LIKE \'{clientId}\') a JOIN (SELECT orderId,SUM (cumQty * sliceAvgPrice)/(CASE WHEN SUM (cumQty)>0 THEN SUM (cumQty) ELSE	NULL END) AS alphaPrice,SUM (cumQty) AS cumQty FROM	ExchangeOrder WHERE	cumQty > 0 GROUP BY	orderId,expireTime) b ON a.orderId = b.orderId	AND a.tradingDay = \'{tradingDay}\') c GROUP BY	side,expireTime"
                cursor.execute(stmt)
                for row in cursor:
                    side.append(row['side'])
                    expireTime.append(row['expireTime'])
                    amount.append(row['amount'])
                    slipage.append(row['slipage'])

        data = pd.DataFrame({'expireTime': expireTime, 'side': side, 'amount': amount, 'slipage': slipage})
        data['expireTime'] = data['expireTime'].map(lambda x: x.strftime('%H:%M:%S'))
        data['amount'] = data['amount'].astype('float')
        data['amount'] = round(data['amount'] / 10000, 2)
        data['slipage'] = data['slipage'].astype('float')
        data['slipage'] = round(data['slipage'] * 10000, 2)
        return data

    def df_stat_bs_amt_bps(self, df):
        list_side = ['buy', 'sell']
        list_amt = []
        list_bps = []
        df_buy = df[df['side'] == 1]
        df_sell = df[df['side'] == 2]
        list_amt.append(round(sum(df_buy['turnover']) / 10000, 2))
        list_amt.append(round(sum(df_sell['turnover']) / 10000, 2))
        list_bps.append(
            0 if sum(df_buy['turnover']) == 0 else round(sum(df_buy['slipageByVwap'] * df_buy['turnover']) / sum(
                df_buy['turnover']) * 10000, 2))
        list_bps.append(
            0 if sum(df_sell['turnover']) == 0 else round(sum(df_sell['slipageByVwap'] * df_sell['turnover']) / sum(
                df_sell['turnover']) * 10000, 2))
        df_sz_amt_bps = pd.DataFrame({'side': list_side, 'amount': list_amt, 'slipage': list_bps})
        return df_sz_amt_bps

    def run(self, tradingDays, clientIds):
        for tradingDay in tradingDays:
            if not os.path.exists(os.path.join(self.tick_path, tradingDay + '.h5')):
                self.logger.error(f'{tradingDay} h5 tick is not existed.')
                continue
            for clientId in clientIds:
                self.logger.info(f'start calculator: {tradingDay}__{clientId}')
                self.email.add_email_content(f'{tradingDay}_({clientId})统计报告，请查收')

                all_clientOrders = self.get_all_clientOrder(tradingDay, clientId)
                total_trade_num = len(all_clientOrders)
                total_turnover = round(sum(all_clientOrders['turnover'] / 10000, 2))
                total_slipage = 0 if sum(all_clientOrders['turnover']) == 0 else round(sum(
                    all_clientOrders['slipageByVwap'] * all_clientOrders['turnover']) / sum(
                    all_clientOrders['turnover']), 2)

                df_total_effect = pd.DataFrame(
                    {'clientId': clientId, '订单数': total_trade_num, '成交额(万元)': total_turnover,
                     '交易效果(bps)': total_slipage}, index=[1])

                all_clientOrders_sh = all_clientOrders[all_clientOrders['exDestination'] == 0]
                all_clientOrders_sz = all_clientOrders[all_clientOrders['exDestination'] == 1]

                all_has_signal_clientOrders = self.get_has_signal_clientOrder(tradingDay, clientId)
                all_has_signal_clientOrders_sh = all_has_signal_clientOrders[
                    all_has_signal_clientOrders['exDestination'] == 0]
                all_has_signal_clientOrders_sz = all_has_signal_clientOrders[
                    all_has_signal_clientOrders['exDestination'] == 1]

                all_nothas_signal_clientOrders = self.get_nothas_signal_clientOrder(tradingDay, clientId)
                all_nothas_signal_clientOrders_sh = all_nothas_signal_clientOrders[
                    all_nothas_signal_clientOrders['exDestination'] == 0]
                all_nothas_signal_clientOrders_sz = all_nothas_signal_clientOrders[
                    all_nothas_signal_clientOrders['exDestination'] == 1]

                list_sh = []
                list_sz = []
                list_sh.append(round(sum(all_clientOrders_sh['turnover']) / 10000, 2))
                list_sz.append(round(sum(all_clientOrders_sz['turnover']) / 10000, 2))
                list_sh.append(len(all_clientOrders_sh))
                list_sz.append(len(all_clientOrders_sz))
                list_sh.append(0 if sum(
                    all_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_clientOrders_sh['slipageByVwap'] * all_clientOrders_sh['turnover']) / sum(
                    all_clientOrders_sh['turnover']), 2))
                list_sz.append(0 if sum(
                    all_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_clientOrders_sz['slipageByVwap'] * all_clientOrders_sz['turnover']) / sum(
                    all_clientOrders_sz['turnover']), 2))

                list_sh.append(sum(all_has_signal_clientOrders_sh['turnover']))
                list_sz.append(sum(all_has_signal_clientOrders_sz['turnover']))
                list_sh.append(0 if sum(
                    all_has_signal_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_has_signal_clientOrders_sh['slipageByVwap'] * all_has_signal_clientOrders_sh['turnover']) / sum(
                    all_has_signal_clientOrders_sh['turnover']), 2))
                list_sz.append(0 if sum(
                    all_has_signal_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_has_signal_clientOrders_sz['slipageByVwap'] * all_has_signal_clientOrders_sz['turnover']) / sum(
                    all_has_signal_clientOrders_sz['turnover']), 2))

                list_sh.append(round(sum(all_nothas_signal_clientOrders_sh['turnover']) / 10000, 2))
                list_sz.append(round(sum(all_nothas_signal_clientOrders_sz['turnover']) / 10000, 2))
                list_sh.append(0 if sum(
                    all_nothas_signal_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_nothas_signal_clientOrders_sh['slipageByVwap'] * all_nothas_signal_clientOrders_sh[
                        'turnover']) / sum(all_nothas_signal_clientOrders_sh['turnover']), 2))
                list_sz.append(0 if sum(
                    all_nothas_signal_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_nothas_signal_clientOrders_sz['slipageByVwap'] * all_nothas_signal_clientOrders_sz[
                        'turnover']) / sum(all_nothas_signal_clientOrders_sz['turnover']), 2))

                df_client_exchange_order = self.join_client_exchange_Order(tradingDay=tradingDay, clientId=clientId)
                df_client_exchange_order_sh = df_client_exchange_order[df_client_exchange_order['exDestination'] == 0]
                df_client_exchange_order_sz = df_client_exchange_order[df_client_exchange_order['exDestination'] == 1]

                # 上海深圳筛选SignalType
                df_client_exchange_order_sh_forward = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'Forward']
                df_client_exchange_order_sh_reverse = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'Reverse']
                df_client_exchange_order_sh_first1 = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'First1']

                df_client_exchange_order_sz_forward = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'Forward']
                df_client_exchange_order_sz_reverse = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'Reverse']
                df_client_exchange_order_sz_first1 = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'First1']

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_forward['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_forward['slipageByVwap'] * df_client_exchange_order_sh_forward[
                        'turnover']) / sum(df_client_exchange_order_sh_forward['turnover']) * 10000, 2))
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_forward['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_forward['slipageByVwap'] * df_client_exchange_order_sz_forward[
                        'turnover']) / sum(df_client_exchange_order_sz_forward['turnover']) * 10000, 2))

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_reverse['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_reverse['slipageByVwap'] * df_client_exchange_order_sh_reverse[
                        'turnover']) / sum(df_client_exchange_order_sh_reverse['turnover']) * 10000, 2))
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_reverse['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_reverse['slipageByVwap'] * df_client_exchange_order_sz_reverse[
                        'turnover']) / sum(df_client_exchange_order_sz_reverse['turnover']) * 10000, 2))

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_first1['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_first1['slipageByVwap'] * df_client_exchange_order_sh_first1[
                        'turnover']) / sum(df_client_exchange_order_sh_first1['turnover']) * 10000, 2))
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_first1['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_first1['slipageByVwap'] * df_client_exchange_order_sz_first1[
                        'turnover']) / sum(df_client_exchange_order_sz_first1['turnover']) * 10000, 2))

                list_summary = ['交易额(万元)', '订单数', '交易效果', '信号交易额(万元)', '信号交易效果', '非信号交易额(万元)', '非信号交易效果',
                                '同向信号交易效果', '反向信号交易效果', '同向首次信号交易效果']
                df_market_effect = pd.DataFrame({'指标': list_summary, 'SZ': list_sz, 'SH': list_sh})

                df_sh_amt_bps = self.df_stat_bs_amt_bps(df_client_exchange_order_sh)
                df_sh_amt_bps.columns = ['方向', '上海总成交金额(万元)', '效果(bps)']

                df_sh_amt_bps_signal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sh[df_client_exchange_order_sh['signalType'] != 'Normal'])
                df_sh_amt_bps_signal.columns = ['方向', '上海信号单成交额(万元)', '效果(bps)']

                df_sh_amt_bps_notsignal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sh[df_client_exchange_order_sh['signalType'] == 'Normal'])
                df_sh_amt_bps_notsignal.columns = ['方向', '上海非信号单成交额(万元)', '效果(bps)']

                df_sz_amt_bps = self.df_stat_bs_amt_bps(df_client_exchange_order_sz)
                df_sz_amt_bps.columns = ['方向', '深圳总成交金额(万元)', '效果(bps)']

                df_sz_amt_bps_signal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sz[df_client_exchange_order_sz['signalType'] != 'Normal'])
                df_sz_amt_bps_signal.columns = ['方向', '深圳信号单成交额(万元)', '效果(bps)']

                df_sz_amt_bps_notsignal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sz[df_client_exchange_order_sz['signalType'] == 'Normal'])
                df_sz_amt_bps_notsignal.columns = ['方向', '深圳非信号单成交额(万元)', '效果(bps)']

                dict_signal_to_bool = {'Normal': '否', 'Signal': '是'}
                df_effect_by_signal = self.stat_effect_by_signal(tradingDay=tradingDay, clientId=clientId)
                df_effect_by_signal['signalType'] = df_effect_by_signal['signalType'].map(
                    lambda x: dict_signal_to_bool.get(x))
                df_effect_by_signal.columns = ['时段', '信号单', '交易额(万元)', '交易效果(bps)']

                dict_int_to_side = {1: '买', 2: '卖'}
                df_effect_by_side = self.stat_effect_by_side(tradingDay=tradingDay, clientId=clientId)
                df_effect_by_side['side'] = df_effect_by_side['side'].map(
                    lambda x: dict_int_to_side.get(x))
                df_effect_by_side.columns = ['时段', '方向', '交易额(万元)', '交易效果(bps)']

                df_receive = self.get_receiveList(clientId)
                df_receive['tradingDay'] = tradingDay
                fileName = f'{tradingDay}_({clientId}).xlsx'
                pathCsv = os.path.join(f'Data/{fileName}')

                ExcelHelper.createExcel(pathCsv)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_total_effect,
                                               header=True, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_market_effect, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sh_amt_bps, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sh_amt_bps_signal, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sh_amt_bps_notsignal, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sz_amt_bps, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sz_amt_bps_signal, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_sz_amt_bps_notsignal, header=True,
                                               interval=3, sheet_name=clientId)

                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_effect_by_signal, header=True,
                                               interval=3, sheet_name=clientId)
                ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_effect_by_side, header=True,
                                               interval=3, sheet_name=clientId)

                ExcelHelper.removeSheet(pathCsv, 'Sheet')
                # self.email.send_email_file(pathCsv, fileName, df_receive)
                self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')


if __name__ == '__main__':
    cfg = RawConfigParser()
    cfg.read('config.ini')
    clientIds = cfg.get('ClientID', 'id')
    start = sys.argv[1]
    end = sys.argv[2]
    reporter = AlgoDetailReporter(start, end, clientIds)
