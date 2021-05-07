#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : AlphaReporter.py
@Time : 2020/10/26 11:27 
"""

import os
import sys
from configparser import RawConfigParser
from Constants import Constants
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


class AlphaReporter(object):
    def __init__(self, tradingDay):
        self.logger = Log.get_logger(__name__)
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(tradingDay, tradingDay)

        cfg = RawConfigParser()
        cfg.read('config.ini', encoding='utf-8')
        clientIds = cfg.get('AlphaReporter', 'id')
        clientIDs = list(clientIds.split(';'))

        self.to_receiver = cfg.get('Email', 'to_receiver')
        self.cc_receiver = cfg.get('Email', 'cc_receiver')

        self.sender = cfg.get('Email', 'sender')
        self.pwd = cfg.get('Email', 'pwd')
        self.post = cfg.get('Email', 'server')

        self.email = EmailHelper.instance()
        self.run(tradingdays, clientIDs)

    def get_connection(self):
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
                stmt = f"select * from ClientOrder where orderid in(SELECT distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId like \'{clientId}\' and tradingDay=\'{tradingday}\' and algo <> 4 and algo <> 6)) order by slipageInBps asc, cumqty desc"
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
                stmt = f"select * from ClientOrder where  clientId like \'{clientId}\' and tradingDay=\'{tradingday}\' and orderid not in(SELECT  distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId like \'{clientId}\' and tradingDay=\'{tradingday}\') and  signalside>0 and algo <> 4 and algo <> 6)  order by slipageInBps asc"
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
                stmt = f"select * from ClientOrder where orderid in(SELECT distinct(orderid) FROM ExchangeOrder where orderid in(SELECT orderid FROM ClientOrder where clientId like \'{clientId}\' and tradingDay=\'{tradingday}\')and  signalside>0 and algo <> 4 and algo <> 6) order by slipageInBps asc, cumqty desc"
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
                stmt = f"SELECT	a.orderId,	a.symbol,	a.tradingDay,	a.side,	b.alphaPrice,	b.cumQty,	a.exDestination AS exDestination,	a.effectiveTime,	a.expireTime,	a.avgPrice,	(CASE	WHEN a.iVWP = 0 THEN 0 ELSE	CASE WHEN a.side = 1 THEN	(a.iVWP - b.alphaPrice) / a.iVWP ELSE (b.alphaPrice - a.iVWP) / a.iVWP	END	END	) AS delta,b.signalType FROM	(SELECT orderId, tradingDay, side, avgPrice, symbol, exDestination, effectiveTime,expireTime,iVWP	FROM ClientOrder WHERE	clientId like \'{clientId}\' and algo <> 4 and algo <> 6) a JOIN (SELECT	orderId,	SUM (cumQty * sliceAvgPrice) / (CASE WHEN SUM (cumQty) > 0 THEN	SUM (cumQty) ELSE	NULL END) AS alphaPrice,SUM (cumQty) AS cumQty,	CASE WHEN (TEXT LIKE '%Sell%LongHolding%' OR TEXT LIKE '%Buy%ShortHolding%'	) THEN 'Reverse' WHEN ( TEXT LIKE '%Buy%LongHolding%' OR TEXT LIKE '%Sell%ShortHolding%' ) THEN 'Forward'	WHEN (TEXT LIKE '%ToNormal%') THEN		'Close'	WHEN decisionType = '22' THEN		'First1'	ELSE		'Normal'	END AS signalType	FROM ExchangeOrder	WHERE		cumQty > 0	GROUP BY		orderId,		CASE	WHEN (		TEXT LIKE '%Sell%LongHolding%'		OR TEXT LIKE '%Buy%ShortHolding%'	) THEN		'Reverse'	WHEN (		TEXT LIKE '%Buy%LongHolding%'	OR TEXT LIKE '%Sell%ShortHolding%') THEN 'Forward' WHEN (TEXT LIKE '%ToNormal%') THEN	'Close'	WHEN decisionType = '22' THEN	'First1' ELSE	'Normal'	END) b ON a.orderId = b.orderId AND a.tradingDay = \'{tradingDay}\'"
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
                stmt = f"SELECT signalType AS signalType, expireTime AS expireTime, SUM (c.cumQty * c.alphaPrice) AS amount, SUM(delta * c.cumQty * c.alphaPrice ) / SUM(c.cumQty * c.alphaPrice) AS slipage FROM(SELECT a.orderId,a.symbol,a.expireTime,a.tradingDay,a.side,b.alphaPrice,b.cumQty,a.avgPrice,a.cumQty AS clientQty,(CASE	WHEN a.iVWP = 0 THEN 0 ELSE	CASE WHEN a.side = 1 THEN	(a.iVWP - b.alphaPrice) / a.iVWP ELSE (b.alphaPrice - a.iVWP) / a.iVWP END END) AS delta,b.signalType FROM(SELECT orderId,tradingDay,side,avgPrice, symbol,iVWP,cumQty,expireTime FROM ClientOrder	WHERE	clientId LIKE \'{clientId}\' and algo <> 4 and algo <> 6) a	JOIN (SELECT orderId,SUM (cumQty * sliceAvgPrice)/(CASE WHEN SUM (cumQty) > 0 THEN	SUM (cumQty)	ELSE	NULL	END) AS alphaPrice,	SUM (cumQty) AS cumQty,	CASE WHEN (signalSide > 0) THEN	'Signal' ELSE 'Normal' END AS signalType FROM	ExchangeOrder WHERE cumQty>0 GROUP BY orderId,CASE WHEN (signalSide > 0) THEN	'Signal' ELSE 'Normal' END,expireTime) b ON a.orderId = b.orderId AND a.tradingDay = \'{tradingDay}\') c GROUP BY signalType,expireTime"
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
                stmt = f"SELECT c.side AS side,expireTime AS expireTime,SUM (c.cumQty * c.alphaPrice) AS amount,	SUM (delta * c.cumQty * c.alphaPrice) / SUM (c.cumQty * c.alphaPrice) AS slipage FROM	(SELECT	a.orderId,a.symbol,a.expireTime,a.tradingDay,	a.side,b.alphaPrice,b.cumQty,a.avgPrice,	a.cumQty AS clientQty,(CASE	WHEN a.iVWP = 0 THEN 0 ELSE	CASE WHEN a.side = 1 THEN	(a.iVWP - b.alphaPrice) / a.iVWP ELSE (b.alphaPrice - a.iVWP) / a.iVWP END END) AS delta FROM	(SELECT	orderId,tradingDay,side,avgPrice,symbol,iVWP,cumQty,expireTime FROM	ClientOrder	WHERE clientId LIKE \'{clientId}\' and algo <> 4 and algo <> 6) a JOIN (SELECT orderId,SUM (cumQty * sliceAvgPrice)/(CASE WHEN SUM (cumQty)>0 THEN SUM (cumQty) ELSE	NULL END) AS alphaPrice,SUM (cumQty) AS cumQty FROM	ExchangeOrder WHERE	cumQty > 0 GROUP BY	orderId,expireTime) b ON a.orderId = b.orderId	AND a.tradingDay = \'{tradingDay}\') c GROUP BY	side,expireTime"
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
            dir_data = os.path.join(f'Data/AlphaReporter/{tradingDay}')
            if not os.path.exists(dir_data):
                os.makedirs(dir_data)

            for clientId in clientIds:
                self.logger.info(f'start calculator: {tradingDay}__{clientId}')
                self.email.add_email_content(f'{tradingDay}_({clientId})统计报告，请查收')

                # 所有订单
                all_clientOrders = self.get_all_clientOrder(tradingDay, clientId)
                total_trade_num = len(all_clientOrders)
                total_turnover = round(sum(all_clientOrders['turnover'] / 10000), 2)
                total_slipage = 0 if sum(all_clientOrders['turnover']) == 0 else round(sum(
                    all_clientOrders['slipageByVwap'] * all_clientOrders['turnover']) / sum(
                    all_clientOrders['turnover']), 2)

                # 表1
                df_total_effect = pd.DataFrame(
                    {'clientId': clientId, '订单数': total_trade_num, '成交额(万元)': total_turnover,
                     '交易效果(bps)': total_slipage}, index=[1])

                all_clientOrders_sh = all_clientOrders[all_clientOrders['exDestination'] == 0]  # 上海所有订单
                all_clientOrders_sz = all_clientOrders[all_clientOrders['exDestination'] == 1]  # 深圳所有订单

                all_has_signal_clientOrders = self.get_has_signal_clientOrder(tradingDay, clientId)  # 所有信号单
                all_has_signal_clientOrders_sh = all_has_signal_clientOrders[
                    all_has_signal_clientOrders['exDestination'] == 0]  # 所有上海信号单
                all_has_signal_clientOrders_sz = all_has_signal_clientOrders[
                    all_has_signal_clientOrders['exDestination'] == 1]  # 所有深圳信号单

                all_nothas_signal_clientOrders = self.get_nothas_signal_clientOrder(tradingDay, clientId)  # 所有非信号单
                all_nothas_signal_clientOrders_sh = all_nothas_signal_clientOrders[
                    all_nothas_signal_clientOrders['exDestination'] == 0]  # 所有上海非信号单
                all_nothas_signal_clientOrders_sz = all_nothas_signal_clientOrders[
                    all_nothas_signal_clientOrders['exDestination'] == 1]  # 所有深圳非信号单

                list_sh = []
                list_sz = []
                list_sh.append(round(sum(all_clientOrders_sh['turnover']) / 10000, 2))  # 上海单总成价额
                list_sz.append(round(sum(all_clientOrders_sz['turnover']) / 10000, 2))  # 深圳单总成价额
                list_sh.append(len(all_clientOrders_sh))  # 上海订单数
                list_sz.append(len(all_clientOrders_sz))  # 深圳订单数
                list_sh.append(0 if sum(
                    all_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_clientOrders_sh['slipageByVwap'] * all_clientOrders_sh['turnover']) / sum(
                    all_clientOrders_sh['turnover']), 2))  # 上海单交易效果
                list_sz.append(0 if sum(
                    all_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_clientOrders_sz['slipageByVwap'] * all_clientOrders_sz['turnover']) / sum(
                    all_clientOrders_sz['turnover']), 2))  # 深圳单交易效果

                list_sh.append(round(sum(all_has_signal_clientOrders_sh['turnover']) / 10000, 2))  # 上海信号单成交额
                list_sz.append(round(sum(all_has_signal_clientOrders_sz['turnover']) / 10000, 2))  # 深圳信号单成交额
                list_sh.append(0 if sum(
                    all_has_signal_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_has_signal_clientOrders_sh['slipageByVwap'] * all_has_signal_clientOrders_sh['turnover']) / sum(
                    all_has_signal_clientOrders_sh['turnover']), 2))  # 上海信号单交易效果
                list_sz.append(0 if sum(
                    all_has_signal_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_has_signal_clientOrders_sz['slipageByVwap'] * all_has_signal_clientOrders_sz['turnover']) / sum(
                    all_has_signal_clientOrders_sz['turnover']), 2))  # 深圳信号单交易效果

                list_sh.append(round(sum(all_nothas_signal_clientOrders_sh['turnover']) / 10000, 2))  # 上海非信号单成交额
                list_sz.append(round(sum(all_nothas_signal_clientOrders_sz['turnover']) / 10000, 2))  # 深圳非信号单成交额
                list_sh.append(0 if sum(
                    all_nothas_signal_clientOrders_sh['turnover']) == 0 else round(sum(
                    all_nothas_signal_clientOrders_sh['slipageByVwap'] * all_nothas_signal_clientOrders_sh[
                        'turnover']) / sum(all_nothas_signal_clientOrders_sh['turnover']), 2))  # 上海非信号单交易效果
                list_sz.append(0 if sum(
                    all_nothas_signal_clientOrders_sz['turnover']) == 0 else round(sum(
                    all_nothas_signal_clientOrders_sz['slipageByVwap'] * all_nothas_signal_clientOrders_sz[
                        'turnover']) / sum(all_nothas_signal_clientOrders_sz['turnover']), 2))  # 深圳非信号单交易效果

                df_client_exchange_order = self.join_client_exchange_Order(tradingDay=tradingDay, clientId=clientId)
                df_client_exchange_order_sh = df_client_exchange_order[df_client_exchange_order['exDestination'] == 0]
                df_client_exchange_order_sz = df_client_exchange_order[df_client_exchange_order['exDestination'] == 1]

                # 上海深圳筛选SignalType
                df_client_exchange_order_sh_forward = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'Forward']  # 上海同向信号订单
                df_client_exchange_order_sh_reverse = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'Reverse']  # 上海反向信号订单
                df_client_exchange_order_sh_first1 = df_client_exchange_order_sh[
                    df_client_exchange_order_sh['signalType'] == 'First1']  # 上海同向首次信号订单

                df_client_exchange_order_sz_forward = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'Forward']  # 深圳同向信号订单
                df_client_exchange_order_sz_reverse = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'Reverse']  # 深圳反向信号订单
                df_client_exchange_order_sz_first1 = df_client_exchange_order_sz[
                    df_client_exchange_order_sz['signalType'] == 'First1']  # 深圳同向首次信号订单

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_forward['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_forward['slipageByVwap'] * df_client_exchange_order_sh_forward[
                        'turnover']) / sum(df_client_exchange_order_sh_forward['turnover']) * 10000, 2))  # 上海同向信号交易效果
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_forward['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_forward['slipageByVwap'] * df_client_exchange_order_sz_forward[
                        'turnover']) / sum(df_client_exchange_order_sz_forward['turnover']) * 10000, 2))  # 深圳同向信号交易效果

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_reverse['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_reverse['slipageByVwap'] * df_client_exchange_order_sh_reverse[
                        'turnover']) / sum(df_client_exchange_order_sh_reverse['turnover']) * 10000, 2))  # 上海反向信号交易效果
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_reverse['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_reverse['slipageByVwap'] * df_client_exchange_order_sz_reverse[
                        'turnover']) / sum(df_client_exchange_order_sz_reverse['turnover']) * 10000, 2))  # 深圳反向信号交易效果

                list_sh.append(0 if sum(
                    df_client_exchange_order_sh_first1['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sh_first1['slipageByVwap'] * df_client_exchange_order_sh_first1[
                        'turnover']) / sum(df_client_exchange_order_sh_first1['turnover']) * 10000, 2))  # 上海同向首次信号交易效果
                list_sz.append(0 if sum(
                    df_client_exchange_order_sz_first1['turnover']) == 0 else round(sum(
                    df_client_exchange_order_sz_first1['slipageByVwap'] * df_client_exchange_order_sz_first1[
                        'turnover']) / sum(df_client_exchange_order_sz_first1['turnover']) * 10000, 2))  # 深圳同向首次信号交易效果

                # 表2
                list_summary = ['交易额(万元)', '订单数', '交易效果', '信号母单交易额(万元)', '信号母单交易效果', '非信号交易额(万元)', '非信号交易效果',
                                '同向信号交易效果', '反向信号交易效果', '同向首次信号交易效果']
                df_market_effect = pd.DataFrame({'指标': list_summary, 'SZ': list_sz, 'SH': list_sh})

                df_sh_amt_bps = self.df_stat_bs_amt_bps(df_client_exchange_order_sh)  # 分买卖上海订单成交额与效果
                df_sh_amt_bps.columns = ['方向', '上海总成交金额(万元)', '效果(bps)']

                df_sh_amt_bps_signal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sh[
                        df_client_exchange_order_sh['signalType'] != 'Normal'])  # 分买卖上海信号单成交额与效果
                df_sh_amt_bps_signal.columns = ['方向', '上海信号子单成交额(万元)', '效果(bps)']

                df_sh_amt_bps_notsignal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sh[
                        df_client_exchange_order_sh['signalType'] == 'Normal'])  # 分买卖上海非信号单成交额与效果
                df_sh_amt_bps_notsignal.columns = ['方向', '上海非信号子单成交额(万元)', '效果(bps)']

                df_sz_amt_bps = self.df_stat_bs_amt_bps(df_client_exchange_order_sz)  # 分买卖深圳订单成交额与效果
                df_sz_amt_bps.columns = ['方向', '深圳总成交金额(万元)', '效果(bps)']

                df_sz_amt_bps_signal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sz[
                        df_client_exchange_order_sz['signalType'] != 'Normal'])  # 分买卖深圳信号单成交额与效果
                df_sz_amt_bps_signal.columns = ['方向', '深圳信号子单成交额(万元)', '效果(bps)']

                df_sz_amt_bps_notsignal = self.df_stat_bs_amt_bps(
                    df_client_exchange_order_sz[
                        df_client_exchange_order_sz['signalType'] == 'Normal'])  # 分买卖深圳非信号单成交额与效果
                df_sz_amt_bps_notsignal.columns = ['方向', '深圳非信号子单成交额(万元)', '效果(bps)']

                dict_signal_to_bool = {'Normal': '否', 'Signal': '是'}
                df_effect_by_signal = self.stat_effect_by_signal(tradingDay=tradingDay, clientId=clientId)
                df_effect_by_signal['signalType'] = df_effect_by_signal['signalType'].map(
                    lambda x: dict_signal_to_bool.get(x))
                df_effect_by_signal.columns = ['时段', '信号单', '交易额(万元)', '交易效果(bps)']  # 时段统计 是否信号单 成交额与效果

                dict_int_to_side = {1: '买', 2: '卖'}
                df_effect_by_side = self.stat_effect_by_side(tradingDay=tradingDay, clientId=clientId)
                df_effect_by_side['side'] = df_effect_by_side['side'].map(
                    lambda x: dict_int_to_side.get(x))  # 时段统计 分买卖 成交额与效果
                df_effect_by_side.columns = ['时段', '方向', '交易额(万元)', '交易效果(bps)']

                df_receive = pd.DataFrame({'to_receiver': self.to_receiver, 'cc_receiver': self.cc_receiver,
                                           'clientName': Constants.dict_id_clientName[clientId], 'clientId': clientId},
                                          index=[1])
                df_receive['tradingDay'] = tradingDay
                fileName = f'AlphaReporter_{tradingDay}_({clientId}).xlsx'
                pathCsv = os.path.join(dir_data, fileName)

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

                self.calSignalEffect(clientId, pathCsv, tradingDay, tradingDay, 0)
                ExcelHelper.removeSheet(pathCsv, 'Sheet')

                self.email.send_email_file(pathCsv, fileName, df_receive, clientId, subject_prefix='AlphaReporter')
                self.email.content = ''
                self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')

    def calSignalEffect(self, clientId, pathCsv, start, end, isclinet):
        # 1.信号效果
        dfSignalEffect = self._statSignalEffect(start, end, clientId, isclinet)
        # 2.订单成交率
        dfTurnoverRatio, dfTurnoverRatiowithQty = self._statTurnOverRatio(start, end, clientId, isclinet)
        dfTurnoverRatio.columns = dfTurnoverRatio.columns.map(lambda x: Constants.PlacementCategoryDict[x])
        dfTurnoverRatiowithQty['category'] = dfTurnoverRatiowithQty['category'].map(
            lambda x: Constants.PlacementCategoryDict[x])
        # 2.1 合并信号效果与订单成交率
        if dfSignalEffect.shape[0] > 0:
            dfSignalEffect = dfSignalEffect.merge(dfTurnoverRatio, left_on='type', right_on=dfTurnoverRatio.index,
                                                  how='left')
            list_columns = ['Id', 'type', 'turnover', 'slipage', 'Aggressive', 'Passive', 'UltraPassive']

            dfSignalEffect = dfSignalEffect[list_columns]
            dfSignalEffect['type'] = dfSignalEffect['type'].map(lambda x: Constants.SingalType2Chn[x])
        # 3. passive/ultraPassive 比例
        try:
            dfRatio = self._statRelativeRate(clientId, dfTurnoverRatiowithQty)
        except Exception as e:
            dfRatio = None
            self.logger.error(e)
            self.logger.error('passive / ultraPassive比例')

        # 4.客户slipage排名
        dfSlipageInBps = self._statSlipageInBps(start, end, clientId, isclinet)
        dfSlipageInBpsWorse20 = dfSlipageInBps.head(20)
        dfSlipageInBpsBetter20 = dfSlipageInBps.tail(20)
        dfSlipageInBpsBetter20.sort_values(by='slipageInBps', axis=0, ascending=False, inplace=True)
        dfSlipageInBpsBetter20 = dfSlipageInBpsBetter20.reset_index(drop=True)

        if not dfSignalEffect is None:
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=dfSignalEffect, header=True, sheet_name=clientId,
                                           interval=3)
        if not dfRatio is None:
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=dfRatio, header=True, interval=4, sheet_name=clientId,
                                           )
        if not dfSlipageInBpsWorse20 is None:
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=dfSlipageInBpsWorse20, header=True, interval=4,
                                           sheet_name=clientId)
        if not dfSlipageInBpsBetter20 is None:
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=dfSlipageInBpsBetter20, header=True, interval=4,
                                           sheet_name=clientId)

    def _statSignalEffect(self, start, end, Id, isClient):
        try:
            clientIds = []
            type = []
            turnover = []
            spread = []
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    proc = 'spu_SignalEffect'
                    cursor.callproc(proc, (Id, start, end, isClient))
                    for row in cursor:
                        clientIds.append(Id)
                        type.append(row['signalType'])
                        turnover.append(row['turnover'])
                        spread.append(row['spread'])

            df = pd.DataFrame({'Id': clientIds, 'type': type, 'turnover': turnover, 'slipage': spread})
            return df

        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def _statTurnOverRatio(self, start, end, clientId, isClient):
        category = []
        cumQty = []
        orderQty = []
        signalType = []
        fillRatio = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_StatTurnoverRatio'
                cursor.callproc(proc, (clientId, start, end, isClient))
                for row in cursor:
                    category.append(row['category'])
                    signalType.append(row['signalType'])
                    cumQty.append([row['cumQty']])
                    orderQty.append([row['Qty']])
                    fillRatio.append(row['fillRatio'])

        df = pd.DataFrame({'category': category, 'signalType': signalType, 'cumQty': cumQty, 'orderQty': orderQty,
                           'fillRatio': fillRatio})
        list_standand = [1, 2, 3]

        diffs = list(set(list_standand).difference(set(df['category'])))
        for index in diffs:
            df = df.append({'category': index, 'signalType': 'Normal', 'cumQty': [0], 'orderQty': [0],
                            'fillRatio': 0},
                           ignore_index=True)

        dfpivot = df.pivot(index='signalType', columns='category', values='fillRatio')
        return dfpivot, df

    def _statRelativeRate(self, id, df):
        try:
            ids = []
            type = []
            normalRate = []
            reveRate = []

            ids.append(id)
            type.append('passive/ultraPassive')

            series_normal_passive = (df[(df['signalType'] == 'Normal') & (df['category'] == 'Passive')]['cumQty'])
            cumQtyNormalPassive = 0 if len(series_normal_passive) == 0 else series_normal_passive.values[0][0]
            series_normal_ultraPassive = (
            df[(df['signalType'] == 'Normal') & (df['category'] == 'UltraPassive')]['cumQty'])
            cumQtyNormalUltraPassive = 0 if len(series_normal_ultraPassive) == 0 else \
            series_normal_ultraPassive.values[0][0]
            normalRate.append(
                0 if cumQtyNormalUltraPassive == 0 else round(cumQtyNormalPassive / cumQtyNormalUltraPassive, 4))

            series_reve_passive = (df[(df['signalType'] == 'Reverse') & (df['category'] == 'Passive')]['cumQty'])
            cumQtyRevePassive = 0 if len(series_reve_passive) == 0 else series_reve_passive.values[0][0]
            series_normal_ultraPassive = (
            df[(df['signalType'] == 'Reverse') & (df['category'] == 'UltraPassive')]['cumQty'])
            cumQtyReveUltraPassive = 0 if len(series_normal_ultraPassive) == 0 else \
                series_normal_ultraPassive.values[0][0]
            reveRate.append(0 if cumQtyReveUltraPassive == 0 else round(cumQtyRevePassive / cumQtyReveUltraPassive, 4))

            df = pd.DataFrame({'id': ids, 'description': type, 'normalRate': normalRate, 'reverseRate': reveRate})
            return df
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def _statSlipageInBps(self, start, end, Id, isClient):
        ids = []
        symbol = []
        side = []
        orderQty = []
        slipageInBps = []
        effectiveTime = []
        expireTime = []
        idtype = "clientId" if isClient == 0 else "accountId"
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                sql = f"SELECT symbol,side, orderQty, slipageInBps, effectiveTime, expireTime FROM ClientOrderView WHERE {idtype} LIKE \'{Id}\' AND tradingDay >= \'{start}\' AND tradingDay <= \'{end}\' AND avgPrice * cumQty > 100000 and algo <> 'POV' and algo <> 'PEGGING' ORDER BY slipageInBps"
                cursor.execute(sql)
                for row in cursor:
                    ids.append(Id)
                    symbol.append(row['symbol'])
                    orderQty.append(row['orderQty'])
                    side.append(row['side'])
                    slipageInBps.append(row['slipageInBps'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])

        df = pd.DataFrame(
            {'Id': ids, 'symbol': symbol, 'side': side, 'orderQty': orderQty, 'slipageInBps': slipageInBps,
             'effectiveTime': effectiveTime, 'expireTime': expireTime})

        df['effectiveTime'] = df['effectiveTime'].map(lambda x: x.strftime('%H:%M:%S'))
        df['expireTime'] = df['expireTime'].map(lambda x: x.strftime('%H:%M:%S'))
        return df


if __name__ == '__main__':
    tradingDay = sys.argv[1]
    reporter = AlphaReporter(tradingDay)
