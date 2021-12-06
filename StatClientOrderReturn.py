#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : StatClientOrderReturn.py 
@Time : 2021/1/12 13:14 
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
from multiprocessing import Pool

from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader
from DataSender.EmailHelper import EmailHelper

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class StatClientOrderReturn(object):
    def __init__(self, start, end, clientId):
        self.logger = Log.get_logger(__name__)
        self.tick_path = "Y:/Data/h5data/stock/tick/"
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None
        self.clientId = clientId
        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)
        self.df_result = pd.DataFrame(
            {'Date': tradingdays, 'turnover_buy': 0, 'turnover_sell': 0, 'return_buy': 0, 'return_sell': 0},
            index=tradingdays)
        self.run(tradingdays)

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
        tradingDays = []
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
                stmt = f"select * from ClientOrderView where cumQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId = \'{clientId}\'"
                cursor.execute(stmt)
                for row in cursor:
                    tradingDays.append(tradingday)
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

        data = pd.DataFrame({'tradingDay': tradingDays, 'orderId': orderId, 'symbol': symbol, 'side': side,
                             'effectiveTime': effectiveTime,
                             'expireTime': expireTime, 'avgPrice': avgprice, 'orderQty': orderQty, 'cumQty': cumQty,
                             'algo': algo, 'orderStatus': orderStatus, 'VWAP': VWAP, 'slipageByVWAP': slipageByVwap})

        data['cumQty'] = data['cumQty'].astype('int')
        data['avgPrice'] = data['avgPrice'].astype('float')
        data['slipageByVWAP'] = data['slipageByVWAP'].astype('float')
        data['VWAP'] = data['VWAP'].astype('float')
        data['turnover'] = data['avgPrice'] * data['cumQty']
        return data

    def read_tick(self, symbol, tradingday):
        """
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame Time类型：93003000 int
        """
        with h5py.File(os.path.join(self.tick_path, ''.join([tradingday, '.h5'])), 'r') as f:
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

    def get_tick_by_symbol(self, tradingDay, symbol, effectiveTime, expireTime, side='Buy'):
        effectiveTime_int = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
        expireTime_int = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
        self.logger.info(f"calcu:{tradingDay}_{symbol}_{effectiveTime_int}_{expireTime_int} order")

        df_tick_symbol = self.read_tick(symbol, tradingDay)
        if (df_tick_symbol.shape[0] == 0):
            raise Exception(f"{tradingDay}__{symbol} tick is null")
        df_tick_symbol_start = df_tick_symbol[df_tick_symbol['Time'] <= effectiveTime_int]
        df_tick_symbol_close = df_tick_symbol[df_tick_symbol['Time'] <= expireTime_int]
        openPx = df_tick_symbol_start.iloc[df_tick_symbol_start.shape[0] - 1, :]['Price']
        closePx = df_tick_symbol_close.iloc[df_tick_symbol_close.shape[0] - 1, :]['Price']
        returnZ = (closePx - openPx) / openPx if side == 'Buy' else (openPx - closePx) / openPx if openPx != 0 else 0
        return returnZ
        # return openPx, closePx, returnZ

    def run(self, tradingDays):
        for tradingDay in tradingDays:
            self.cal_client_order_return(tradingDay)
        self.df_result.to_csv('retult.csv')

    def cal_client_order_return(self, tradingDay):
        if not os.path.exists(os.path.join(self.tick_path, tradingDay + '.h5')):
            self.logger.error(f'{tradingDay} h5 tick is not existed.')
            return
        self.logger.info(f'start calculator: {tradingDay}__{self.clientId}')
        clientOrders = self.get_clientOrder(tradingDay, self.clientId)
        if clientOrders.shape[0] == 0:
            return
        clientOrders['return'] = clientOrders.apply(
            lambda x: self.get_tick_by_symbol(x['tradingDay'], x['symbol'], x['effectiveTime'], x['expireTime'],
                                              x['side']), axis=1)

        clientOrders_buy = clientOrders[clientOrders['side'] == 'Buy']
        clientOrders_sell = clientOrders[clientOrders['side'] == 'Sell']
        turnover_buy = round(sum(clientOrders_buy['turnover']), 3)
        turnover_sell = round(sum(clientOrders_sell['turnover']), 3)
        return_buy = round(
            sum(clientOrders_buy['turnover'] * clientOrders_buy['return']) / sum(clientOrders_buy['turnover']), 5)
        return_sell = round(sum(clientOrders_sell['turnover'] * clientOrders_sell['return']) / sum(
            clientOrders_sell['turnover']), 5)

        self.df_result.loc[tradingDay, 'turnover_buy'] = turnover_buy
        self.df_result.loc[tradingDay, 'turnover_sell'] = turnover_sell
        self.df_result.loc[tradingDay, 'return_buy'] = return_buy
        self.df_result.loc[tradingDay, 'return_sell'] = return_sell
        print(self.df_result)
        self.logger.info(f'calculator: {tradingDay}__{id} successfully')


if __name__ == '__main__':
    start = '20201230'
    end = '20201230'
    clientId = "Cld_TRX_5001016"
    reporter = StatClientOrderReturn(start, end, clientId)
