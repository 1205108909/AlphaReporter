#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : CorrelationOPxAndAvgPx.py 
@Time : 2021/1/19 10:48 
"""

import pymssql
import pandas as pd
import h5py
import sys
import Log
import os
import numpy as np
from configparser import RawConfigParser
import datetime

from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class CorrelationOPxAndAvgPx(object):
    def __init__(self, start, end, clientId):
        self.logger = Log.get_logger(__name__)
        self.tick_path = "Y:/Data/h5data/stock/tick/"
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)
        self.result = pd.DataFrame()
        self.run(tradingdays, clientId)

    def get_connection(self):
        try:
            self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
            return self.conn
        except pymssql.OperationalError as e:
            print(e)

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

    def run(self, tradingDays, clientId):
        def cal_openPx(tradingDay, effectiveTime, symbol, cumQty):
            if cumQty == 0: return 0
            effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            self.logger.info(f'cal_openPx-{tradingDay}-{effectiveTime}-{symbol}')
            tick = read_tick(symbol, tradingDay)
            tick = tick[tick['Time'] >= effectiveTime]
            return tick.head(1).iloc[0, :]['Price']

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
            self.logger.info(f'start calculator: {tradingDay}__{clientId}')
            clientOrders = self.get_clientOrder(tradingDay, clientId)
            if clientOrders.size == 0:
                continue

            # 3.计算OrderOpenPx
            clientOrders['openPx'] = clientOrders.apply(
                lambda x: cal_openPx(tradingDay, x['effectiveTime'], x['symbol'], x['cumQty']), axis=1)

            clientOrders['return'] = clientOrders.apply(
                lambda x: x['avgPrice'] - x['openPx'] if x['side'] == 'Sell' else x['openPx'] - x['avgPrice'], axis=1)
            clientOrders['isWin'] = clientOrders['return'] >= 0
            df_group = clientOrders.groupby(['side', 'isWin'])['turnover'].agg(['count', 'sum'])
            df_group = df_group.unstack()
            df_group.columns = ['loss_count', 'win_count', 'loss_turnover(万)', 'win_turnover(万)']
            df_group['loss_turnover(万)'] = round(df_group['loss_turnover(万)'] / 10000, 2)
            df_group['win_turnover(万)'] = round(df_group['win_turnover(万)'] / 10000, 2)
            df_group['TradingDay'] = tradingDay
            self.result = self.result.append(df_group)
            self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')
        self.result.to_csv(f'CorrelationOPxAndAvgPx_{start}_{end}_{clientId}.csv')


if __name__ == '__main__':
    # end = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    start = '20201223'
    end = '20210118'
    clientId = 'Cld_TRX_5001016'
    reporter = CorrelationOPxAndAvgPx(start, end, clientId)
