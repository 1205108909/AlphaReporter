#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : DailyReporter.py 
@Time : 2021/6/7 10:33 
@FileDescription:
"""

import datetime
import os
import sys
from decimal import Decimal

import cx_Oracle
import h5py
import numpy as np
import pandas as pd
import pymssql

import Log

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class AccountInfo(object):
    def __init__(self, clientName, accountId, accountName, isUseHcAlgo, isStatArb):
        self.clientName = clientName
        self.accountId = accountId
        self.accountName = accountName
        self.isUseHcAlgo = isUseHcAlgo
        self.isStatArb = isStatArb


class DailyReporter(object):
    def __init__(self, date):
        self.date = date
        self.logger = Log.get_logger(__name__)
        self.initVar()
        self.initInVar()

    def initVar(self):
        """:arg 初始化变量"""
        self.linkXUser_conn_string = 'LinkXUser/Abcd123456@172.10.10.8:1521/orcl'
        self.fundUser_conn_string = 'FundUser/Abcd123456@172.10.10.8:1522/orcl'
        self.tick_path = "Y:/Data/h5data/stock/tick/"

        self.dir_engine_by_signal = {
            "ALP": ['10.5.91.45-9997', '10.5.91.45-9998', '10.5.91.45-9999', '10.5.91.45-9990', '10.5.91.46-9997',
                    '10.5.91.46-9996', '10.5.91.46-9995', '10.5.91.56-9999'],
            'RTS2': ['10.5.91.46-9994', '10.5.91.47-9996', '10.5.91.47-9995', '10.5.91.56-9998', '10.5.91.56-9997',
                     '10.5.91.56-9996', '10.5.91.56-9995'],
            'TRM2': ['10.5.91.47-9997', '10.5.91.47-9998', '10.5.91.47-9999', '10.5.91.57-9995', '10.5.91.57-9996',
                     '10.5.91.57-9997', '10.5.91.57-9998', '10.5.91.57-9999'],
            'OFM2': ['10.5.91.47-9997', '10.5.91.47-9998', '10.5.91.47-9999', '10.5.91.57-9995', '10.5.91.57-9996',
                     '10.5.91.57-9997', '10.5.91.57-9998', '10.5.91.57-9999']
        }

        self.dir_engine_by_clientId = {
            "泰铼投资": ['10.5.91.45-9999', '10.5.91.45-9998', '10.5.91.45-9997', '10.5.91.45-9899', '10.5.91.45-9990',
                     '10.5.91.46-9994', '10.5.91.47-9999', '10.5.91.47-9998', '10.5.91.47-9997', '10.5.91.47-9996',
                     '10.5.91.47-9995', '10.5.91.53-9999', '10.5.91.53-9998', '10.5.91.53-9997', '10.5.91.53-9996',
                     '10.5.91.53-9995', '10.5.91.53-9994', '10.5.91.53-9993', '10.5.91.53-9992', '10.5.91.53-9991',
                     '10.5.91.53-9990'],
            '富善投资': ['10.5.91.46-9997', '10.5.91.46-9996', '10.5.91.46-9995', '10.5.91.56-9999', '10.5.91.56-9998',
                     '10.5.91.56-9997', '10.5.91.56-9996', '10.5.91.56-9995',
                     '10.5.91.57-9999', '10.5.91.57-9998', '10.5.91.57-9997', '10.5.91.57-9996', '10.5.91.57-9995'],
        }

        accounts = [
            AccountInfo(clientName='富善投资', accountId='7001007', accountName='安享富利宝', isUseHcAlgo=True,
                        isStatArb=True),
            AccountInfo(clientName='富善投资', accountId='6001012', accountName='安享富利宝', isUseHcAlgo=True, isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='6001008', accountName='安享尊享3号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='7001008', accountName='安享尊享3号', isUseHcAlgo=True, isStatArb=True),
            AccountInfo(clientName='富善投资', accountId='6001009', accountName='安享尊享4号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='7001009', accountName='安享尊享4号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='6001011', accountName='安享尊享5号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='7001006', accountName='安享尊享5号', isUseHcAlgo=True, isStatArb=True),
            AccountInfo(clientName='富善投资', accountId='6001010', accountName='安享尊享6号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='7001005', accountName='安享尊享6号', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='6001007', accountName='致远CTA进取七期', isUseHcAlgo=True,
                        isStatArb=False),
            AccountInfo(clientName='富善投资', accountId='7001004', accountName='致远CTA进取七期', isUseHcAlgo=True,
                        isStatArb=True),

            AccountInfo(clientName='泰铼投资', accountId='7001030', accountName='创泰1号', isUseHcAlgo=False, isStatArb=False),
            AccountInfo(clientName='泰铼投资', accountId='7001050', accountName='创泰2号', isUseHcAlgo=True, isStatArb=False),
            AccountInfo(clientName='泰铼投资', accountId='6001055', accountName='联泰1号', isUseHcAlgo=False, isStatArb=False),
            AccountInfo(clientName='泰铼投资', accountId='6001070', accountName='联泰2号', isUseHcAlgo=False, isStatArb=False),
            AccountInfo(clientName='泰铼投资', accountId='6001044', accountName='金泰1号', isUseHcAlgo=False, isStatArb=False)
        ]

        self.accounts = accounts

        self.colume_name_chn = {'client': '客户名称', 'accountName': '账户名称', 'accountId': 'AcctId',
                                'isUseHcAlgo': '是否使用华创算法', 'isStatArb': '是否为统计套利', 'totalAsset': '总资产(万元)',
                                'transVol': '交易额(万元)', 'VWAP': 'VWAP(bps)', 'TWAP': 'TWAP(bps)',
                                'alpSTransVolRatio': 'ALP同向成交额占比', 'alpSBps': 'ALP同向滑点',
                                'alpRTransVolRatio': 'ALP反向成交额占比', 'alpRBps': 'ALP反向滑点',
                                'rts2STransVolRatio': 'RTS2同向成交额占比', 'rts2SBps': 'RTS2同向滑点',
                                'rts2RTransVolRatio': 'RTS2反向成交额占比', 'rts2RBps': 'RTS2反向滑点',
                                'trm2STransVolRatio': 'TRM2同向成交额占比', 'trm2SBps': 'TRM2同向滑点',
                                'trm2RTransVolRatio': 'TRM2反向成交额占比', 'trm2RBps': 'TRM2反向滑点',
                                'ofm2STransVolRatio': 'OFM2同向成交额占比', 'ofm2SBps': 'OFM2同向滑点',
                                'ofm2RTransVolRatio': 'OFM2反向成交额占比', 'ofm2RBps': 'OFM2反向滑点',
                                'cancelrateTransVolRatio': 'CancelRate成交额占比', 'cancelrateBps': 'CancelRate滑点',
                                }

        self.bool_chn = {True: '是', False: '否'}

        clients = []
        accountIds = []
        accountNames = []
        isUseHcAlgos = []
        isStatArbs = []

        for account in accounts:
            clients.append(account.clientName)
            accountIds.append(account.accountId)
            accountNames.append(account.accountName)
            isUseHcAlgos.append(account.isUseHcAlgo)
            isStatArbs.append(account.isStatArb)

        result = pd.DataFrame(
            {'client': clients, 'accountId': accountIds, 'accountName': accountNames, 'isUseHcAlgo': isUseHcAlgos,
             'isStatArb': isStatArbs})

        result['totalAsset'] = 0
        result['transVol'] = 0
        result['VWAP'] = 0
        result['TWAP'] = 0
        result['alpSTransVolRatio'] = 0
        result['alpSBps'] = 0
        result['alpRTransVolRatio'] = 0
        result['alpRBps'] = 0
        result['rts2STransVolRatio'] = 0
        result['rts2SBps'] = 0
        result['rts2RTransVolRatio'] = 0
        result['rts2RBps'] = 0
        result['trm2STransVolRatio'] = 0
        result['trm2SBps'] = 0
        result['trm2RTransVolRatio'] = 0
        result['trm2RBps'] = 0
        result['ofm2STransVolRatio'] = 0
        result['ofm2SBps'] = 0
        result['ofm2RTransVolRatio'] = 0
        result['ofm2RBps'] = 0
        result['cancelrateTransVolRatio'] = 0
        result['cancelrateBps'] = 0

        result.set_index('accountId', inplace=True)

        self.result = result
        # self.df_engine_amt = self.get_engine_totalAmt(date)

    def initInVar(self):
        """:arg 初始化常量"""
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.port = 1433
        self.conn = None
        self.threadNum = 16

    def get_connection(self):
        """:arg 建立AlgoTradingReport数据库"""
        try:
            self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
            return self.conn
        except pymssql.OperationalError as e:
            print(e)

    def get_fundUser_connection(self):
        """:arg 建立富善订单所在数据库连接"""
        try:
            self.conn = cx_Oracle.connect(self.fundUser_conn_string)
            return self.conn
        except Exception as e:
            print(e)

    def get_linkXUser_connection(self):
        """:arg 建立泰铼订单所在数据库连接"""
        try:
            self.conn = cx_Oracle.connect(self.linkXUser_conn_string)
            return self.conn
        except Exception as e:
            print(e)

    def get_clientOrder(self, tradingday, account):
        """
        get_clientOrder 获取accountID下的母单
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        if account.isUseHcAlgo:
            data = self.get_clientOrder_algo(tradingday, account)
            if len(data) == 0:
                return pd.DataFrame()
        else:
            data = self.get_clientOrder_tradeX(tradingday, account)
            if len(data) == 0:
                return pd.DataFrame()

            group = data.groupby(['symbol', 'effectiveTime'])
            df = pd.DataFrame({})
            df['avgPrice'] = group['turnover'].sum() / group['cumQty'].sum()
            df['side'] = group['side'].first()
            df['turnover'] = group['turnover'].sum()
            df['effectiveTime'] = group['effectiveTime'].first()
            df['expireTime'] = group['expireTime'].first()
            df['symbol'] = group['symbol'].first()
            df['cumQty'] = group['cumQty'].sum()

            data = df

            data['VWAP'] = data.apply(
                lambda x: self.cal_vwap(tradingday, x['effectiveTime'], x['expireTime'], x['symbol'], x['cumQty'],
                                        x['avgPrice']), axis=1)
            data['slipageByVWAP'] = data.apply(
                lambda x: self.cal_vwap_slipage(x['VWAP'], x['side'], x['avgPrice']), axis=1)
            # data.loc[data.loc[:, 'cumQty'] == 0, ['slipageByTWAP']] = 0.00
            data['VWAP'] = round(data['VWAP'], 5)
            data['slipageByVWAP'] = round(data['slipageByVWAP'] * 10000, 2)

        data['TWAP'] = data.apply(
            lambda x: self.cal_twap(tradingday, x['effectiveTime'], x['expireTime'], x['symbol'], x['cumQty'],
                                    x['avgPrice']), axis=1)

        # 2.计算slipageByTwap
        data['slipageByTWAP'] = data.apply(
            lambda x: self.cal_twap_slipage(x['TWAP'], x['side'], x['avgPrice']), axis=1)
        data['TWAP'] = round(data['TWAP'], 5)
        data['slipageByTWAP'] = round(data['slipageByTWAP'] * 10000, 2)
        return data

    def get_clientOrder_algo(self, tradingday, account):
        """
        get_clientOrder_algo 获取algo数据库下订单
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        accountId = account.accountId
        orderId = []
        symbol = []
        side = []
        instances = []
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
                stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and accountId like \'{accountId}\' AND algo <> 'POV' AND algo <> 'PEGGING' AND (clientId like '%5001016' or clientId like '%5001017' or clientId like '%5001019' or clientId like '%5001093')"
                # stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId like \'{clientId}\' and accountId like \'{accountId}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
                self.logger.info(stmt)
                cursor.execute(stmt)
                for row in cursor:
                    orderId.append(row['orderId'])
                    symbol.append(row['symbol'])
                    side.append(row['side'])
                    effectiveTime.append(row['effectiveTime'])
                    expireTime.append(row['expireTime'])
                    instances.append(row['Instance'])
                    avgprice.append(row['avgPrice'])
                    cumQty.append(row['cumQty'])
                    slipageByVwap.append(row['slipageInBps'])
                    algo.append(row['algo'])
                    orderQty.append(row['orderQty'])
                    orderStatus.append(row['orderStatus'])
                    if row['iVWP'] > 0:
                        VWAP.append(row['iVWP'])
                    else:
                        VWAP.append(row['avgPrice'])

        data = pd.DataFrame({'orderId': orderId, 'symbol': symbol, 'side': side, 'effectiveTime': effectiveTime,
                             'expireTime': expireTime, 'avgPrice': avgprice, 'orderQty': orderQty,
                             'cumQty': cumQty, 'instance': instances,
                             'algo': algo, 'orderStatus': orderStatus, 'VWAP': VWAP,
                             'slipageByVWAP': slipageByVwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgPrice'] = data['avgPrice'].astype('float')
        data['slipageByVWAP'] = data['slipageByVWAP'].astype('float')
        data['VWAP'] = data['VWAP'].astype('float')
        data['turnover'] = data['avgPrice'] * data['cumQty']

        data['effectiveTime'] = data['effectiveTime'].map(
            lambda x: x.hour * 10000000 + x.minute * 100000 + x.second * 1000)
        data['expireTime'] = data['expireTime'].map(lambda x: x.hour * 10000000 + x.minute * 100000 + x.second * 1000)
        return data

    def get_clientOrder_tradeX(self, tradingday, account):
        """
        get_clientOrder_tradeX 获取TradeX数据库中的订单
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        accountId = account.accountId
        symbols = []
        tradingDays = []
        times = []
        sides = []
        avgPrices = []
        orderPrices = []
        orderQtys = []
        cumQtys = []

        if account.clientName == '富善投资':
            conn = self.get_fundUser_connection()
        else:
            conn = self.get_linkXUser_connection()
        with conn.cursor() as cursor:
            stmt = f"select symbol as symbol, to_char(tradingday,'yyyymmdd') as tradingDay, to_char(ordertime,'hh24miss') as time, orderside as side, avgprice as avgPrice, orderprice as orderPrice, orderqty as orderQty, cumqty as cumQty from tra_clientorder where tradingDay = to_timestamp(\'{tradingday}\','yyyymmdd') and accountId = \'{accountId}\'"
            self.logger.info(stmt)
            cursor.execute(stmt)
            for row in cursor:
                symbols.append(row[0])
                tradingDays.append(row[1])
                times.append(row[2])
                sides.append(row[3])
                avgPrices.append(row[4])
                orderPrices.append(row[5])
                orderQtys.append(row[6])
                cumQtys.append(row[7])

        conn.close()
        data = pd.DataFrame({'symbol': symbols, 'tradingDay': tradingDays, 'time': times, 'side': sides,
                             'avgPrice': avgPrices, 'orderPrice': orderPrices, 'orderQty': orderQtys,
                             'cumQty': cumQtys})

        data['time'] = data['time'].astype('int')
        data['time'] = data['time'] * 1000
        data['effectiveTime'] = data['time'].apply(lambda t: 140700000 if t < 144200000 else 144200000)
        data['expireTime'] = data['time'].apply(lambda t: 144200000 if t < 144200000 else 145700000)

        data['orderQty'] = data['orderQty'].astype('int')
        data['cumQty'] = data['cumQty'].astype('int')
        data['avgPrice'] = data['avgPrice'].astype('float')
        data['orderPrice'] = data['orderPrice'].astype('float')
        data['turnover'] = data['avgPrice'] * data['cumQty']

        data['symbol'] = data['symbol'].map(lambda x: x + '.sh' if x.startswith('6') else x + '.sz')
        return data

    def cal_vwap(self, tradingDay, effectiveTime, expireTime, symbol, cumQty, avgprice):
        """:arg 计算vwap"""
        if cumQty == 0:
            return 0

        self.logger.info(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol}')
        tick = self.get_tick_by_symbol(tradingDay, symbol, effectiveTime, expireTime)
        return tick['Turnover'].sum() / tick['Volume'].sum() if tick.size > 0 else avgprice

    def cal_twap(self, tradingDay, effectiveTime, expireTime, symbol, cumQty, avgPrice):
        """:arg 计算twap"""
        try:
            if cumQty == 0:
                return 0
            # effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            # expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol}')
            data = self.get_tick_by_symbol(tradingDay, symbol, effectiveTime, expireTime)
            return data.Price.sum() / data.Volume.count() if data.size > 0 else avgPrice
        except Exception as e:
            print(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol} error')
            print(e)

    def cal_twap_slipage(self, twap, side, avgprice):
        """:arg 计算twap_slipage"""
        avgprice = np.float64(avgprice)
        slipageByTwap = 0.00 if twap == 0.00 else (
            (avgprice - twap) / twap if 'sell' in side.lower() else (twap - avgprice) / twap)
        return slipageByTwap

    def cal_vwap_slipage(self, vwap, side, avgprice):
        """:arg 计算vwap_slipage"""
        avgprice = np.float64(avgprice)
        slipageByVwap = 0.00 if vwap == 0.00 else (
            (avgprice - vwap) / vwap if 'sell' in side.lower() else (vwap - avgprice) / vwap)
        return slipageByVwap

    def cal_daily_report(self, account):
        """:arg 主函数"""
        # 1.统计账户总资产
        total_asset = self.stat_total_asset(account)
        self.result.loc[account.accountId, 'totalAsset'] = round(total_asset / 10000, 2)

        df_client_order = self.get_clientOrder(self.date, account)

        if len(df_client_order) == 0:
            return

        # 2.统计账户成交额
        trade_vol = sum(df_client_order['turnover'])
        self.result.loc[account.accountId, 'transVol'] = round(trade_vol / 10000, 2)

        # 3.统计账户VWAP与TWAP
        profile_vwp_bps = 0 if sum(df_client_order['turnover']) == 0 else round(sum(
            df_client_order['slipageByVWAP'] * df_client_order['turnover']) / sum(
            df_client_order['turnover']), 2)

        profile_twp_bps = 0 if sum(df_client_order['turnover']) == 0 else round(sum(df_client_order['slipageByTWAP'] *
                                                                                    df_client_order['turnover']) / sum(
            df_client_order['turnover']), 2)

        self.result.loc[account.accountId, 'VWAP'] = profile_vwp_bps
        self.result.loc[account.accountId, 'TWAP'] = profile_twp_bps

        df_alpha_amt_bps = self.stat_alpha_amt_bps(account.accountId, self.date, self.date)
        if len(df_alpha_amt_bps) == 0:
            return
        df_alpha_amt_bps['type'], df_alpha_amt_bps['side'] = zip(*df_alpha_amt_bps['placeReason'].apply(
            lambda x: self.get_signal_type_side(x)))

        df_alpha_amt_bps = df_alpha_amt_bps.groupby(['type', 'side']).apply(self.merge_amt_bps)

        self.dir_instance_signal_amt = {}
        for key, value in self.dir_engine_by_signal.items():
            self.dir_instance_signal_amt[key] = sum(
                df_client_order[df_client_order['instance'].isin(value)]['turnover']) / 10000

        self.dir_instance_clientId_amt = {}
        for key, value in self.dir_engine_by_clientId.items():
            self.dir_instance_clientId_amt[key] = sum(
                df_client_order[df_client_order['instance'].isin(value)]['turnover']) / 10000

        df_alpha_amt_bps['ratio'] = df_alpha_amt_bps.apply(lambda x: self.cal_alpha_amt_ratio(x, account.clientName),
                                                           axis=1)

        if len(df_alpha_amt_bps) > 0:
            for type in self.dir_engine_by_signal.keys():
                for side in ['R', 'S']:
                    df_type_side = df_alpha_amt_bps[
                        (df_alpha_amt_bps['type'] == type) & (df_alpha_amt_bps['side'] == side)]
                    if len(df_type_side) > 0:
                        self.result.loc[account.accountId, f'{type.lower()}{side}TransVolRatio'] = df_type_side.iloc[0][
                            'ratio']
                        self.result.loc[account.accountId, f'{type.lower()}{side}Bps'] = df_type_side.iloc[0]['slipage']

            special_type = 'CANCELRATE'
            if special_type in list(df_alpha_amt_bps['type']):
                side = ''
                df_type_side = df_alpha_amt_bps[(df_alpha_amt_bps['type'] == special_type)]
                if len(df_type_side) > 0:
                    self.result.loc[account.accountId, f'{special_type.lower()}{side}TransVolRatio'] = df_type_side.iloc[0][
                        'ratio']
                    self.result.loc[account.accountId, f'{special_type.lower()}{side}Bps'] = df_type_side.iloc[0]['slipage']

    def merge_amt_bps(self, x):
        """:arg 根据side，type聚合记录"""
        d = {}
        d['type'] = x['type'].max()
        d['side'] = x['side'].max()

        d['accountId'] = x['accountId'].max()
        d['placeReason'] = x['placeReason'].max()
        d['slipage'] = 0 if sum(x['alphaCumAmt']) == 0 else round(sum(x['slipage'] * x['alphaCumAmt']) / sum(
            x['alphaCumAmt']), 2)
        d['alphaCumAmt'] = x['alphaCumAmt'].sum()
        d['cumAmt'] = x['cumAmt'].sum()
        return pd.Series(d, index=['accountId', 'placeReason', 'slipage', 'alphaCumAmt', 'cumAmt', 'type', 'side'])

    def get_signal_type_side(self, placeReason):
        """:arg 根据placeReason解析side，type"""
        alpha_type = np.NAN
        alpha_side = np.NAN
        if placeReason != '':
            list_place_reason = placeReason.split('_')
            if len(list_place_reason) > 1:
                type = placeReason.split('_')[0]
                if type in self.dir_engine_by_signal.keys():
                    alpha_type = type
                    alpha_side = placeReason.split('_')[1]
        if placeReason == 'CANCEL_RATE':
            alpha_type = 'CANCELRATE'
            alpha_side = ''
        return alpha_type, alpha_side

    def cal_alpha_amt_ratio(self, row, clientName):
        """:arg 计算alpha的amt和ratio"""
        if len(row) > 0:
            if row['placeReason'] == 'CANCEL_RATE':
                clientId_type_total_amt = self.dir_instance_clientId_amt[clientName]
                return 0 if clientId_type_total_amt == 0 else str(
                    round(row['alphaCumAmt'] / Decimal(clientId_type_total_amt) * 100, 2)) + '%'

            else:
                alpha_type = row['placeReason'].split('_')[0]
                if alpha_type in self.dir_engine_by_signal.keys():
                    alpha_type_total_amt = self.dir_instance_signal_amt[alpha_type]
                    return 0 if alpha_type_total_amt == 0 else str(
                        round(row['alphaCumAmt'] / Decimal(alpha_type_total_amt) * 100, 2)) + '%'
                else:
                    return np.NAN
        else:
            return np.NAN

    def get_engine_totalAmt(self, tradingDay):
        try:
            engineNames = []
            totalAmts = []
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    stmt = f'SELECT b.instance AS engineName,SUM (a.avgprice * a.cumQty) AS totalAmt FROM ClientOrder a JOIN EngineList b ON a.instance = b.SeqNb WHERE a.tradingDay = \'{tradingDay}\' GROUP BY b.instance'
                    self.logger.info(stmt)
                    cursor.execute(stmt)
                    for row in cursor:
                        engineNames.append(row['engineName'])
                        totalAmts.append(row['totalAmt'])

            df = pd.DataFrame({'engineName': engineNames, 'totalAmt': totalAmts})
            return df

        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def stat_alpha_amt_bps(self, accountId, start, end):
        """:arg 统计alpha信号的amt和bps"""
        try:
            placeReasons = []
            alphaCumAmts = []
            slipages = []
            cumAmts = []
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    proc = 'spu_StatAlphaAmtBps'
                    cursor.callproc(proc, (accountId, start, end))
                    for row in cursor:
                        placeReasons.append(row['placeReason'])
                        alphaCumAmts.append(row['alphaCumAmt'])
                        slipages.append(row['Slipage'])
                        cumAmts.append(row['cumAmt'])

            df = pd.DataFrame(
                {'accountId': accountId, 'placeReason': placeReasons, 'alphaCumAmt': alphaCumAmts, 'slipage': slipages,
                 'cumAmt': cumAmts})
            return df

        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def stat_total_asset(self, account):
        """:arg 统计account的总资产"""
        accountId = account.accountId

        if account.clientName == '富善投资':
            conn = self.get_fundUser_connection()
        else:
            conn = self.get_linkXUser_connection()

        with conn.cursor() as cursor:
            if accountId.startswith('60'):
                stmt = f'select * from TRA_SecFund where accountId = \'{accountId}\''
                self.logger.info(stmt)
                cursor.execute(stmt)
                for row in cursor:
                    total_asset = row[13] + row[19]  # 13:ASSETBALANCE,19:HKMARKETVALUE
                    break
            else:
                stmt = f'select * from TRA_CreditAssets where accountId = \'{accountId}\''
                cursor.execute(stmt)
                self.logger.info(stmt)
                for row in cursor:
                    total_asset = row[5] + row[4]  # 4.'MARKET_VALUE' 5.'CASH_ASSET'
                    break
        conn.close()
        return total_asset

    def get_tick_by_symbol(self, tradingDay, symbol, startTime=90000000, endTime=160000000):
        """:arg 获得一段时间内的tick"""
        df_tick_symbol = self.read_symbol_tick(tradingDay, symbol)
        if df_tick_symbol.shape[0] == 0:
            return pd.DataFrame()
        return df_tick_symbol[(df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                df_tick_symbol['Volume'] > 0)]

    def read_symbol_tick(self, tradingday, symbol):
        """:arg 读取tick"""
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

    def multiRun(self):
        # with Pool(processes=self.threadNum) as pool:
        #     pool.map(self.cal_daily_report, self.accounts)
        start_time = datetime.datetime.now()
        for account in self.accounts:
            self.logger.info(f'{account.accountId} start')
            self.cal_daily_report(account)
            self.logger.info(f'{account.accountId} end')

        dir_data = os.path.join(f'Data/DailyReporter/')
        if not os.path.exists(dir_data):
            os.makedirs(dir_data)

        self.result['isUseHcAlgo'] = self.result['isUseHcAlgo'].map(lambda x: self.bool_chn[x])
        self.result['isStatArb'] = self.result['isStatArb'].map(lambda x: self.bool_chn[x])
        self.result.rename(columns=self.colume_name_chn, inplace=True)
        self.result.to_csv(f'{dir_data}/{self.date}.csv', encoding='utf_8_sig')
        end_time = datetime.datetime.now()
        self.logger.info(end_time - start_time)


if __name__ == '__main__':
    date = sys.argv[1]
    reporter = DailyReporter(date)
    reporter.multiRun()
