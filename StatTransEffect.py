#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : StatTransEffect.py 
@Time : 2021/1/7 13:14
统计aggressive单成交效果
"""

import os
import sys
from Constants import Constants
import h5py
import pandas as pd
import pymssql

import Log
from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class StatTransEffect(object):
    def __init__(self, start, end, clientId):
        self.logger = Log.get_logger(__name__)
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)

        self.df_result = pd.DataFrame(
            {'Date': tradingdays, 'trade_num': 0, 'turnover': 0, 'VWAPbps': 0, 'sh_trade_num': 0, 'sh_turnover': 0,
             'sh_VWAPbps': 0,
             'sz_trade_num': 0, 'sz_turnover': 0, 'sz_VWAPbps': 0, 'sh_aggressive_turnover': 0,
             'sh_aggressive_ratio': 0,
             'sh_aggressive_num': 0, 'sz_aggressive_turnover': 0, 'sz_aggressive_ratio': 0, 'sz_aggressive_num': 0,
             'sh_not_aggressive_turnover': 0, 'sh_not_aggressive_ratio': 0, 'sh_not_aggressive_num': 0,
             'sz_not_aggressive_turnover': 0, 'sz_not_aggressive_ratio': 0, 'sz_not_aggressive_num': 0},
            index=tradingdays)
        self.run(tradingdays, clientId)

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

    def join_ExchangeOrder_category(self, tradingday, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        orderId = []
        price = []
        sliceAvgPrice = []
        cumQty = []
        category = []
        exDestination = []
        symbol = []
        tradingdays = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f'SELECT a.orderId,a.price,a.sliceAvgPrice,a.cumQty,a.category,b.exDestination,b.symbol,b.tradingDay FROM (SELECT orderId,price,cumQty,category,sliceAvgPrice FROM ExchangeOrder	WHERE cumQty > 0) a JOIN (SELECT orderId,symbol,tradingDay,exDestination FROM ClientOrder	WHERE clientId = \'{clientId}\') b ON a.orderId = b.orderId AND b.tradingDay = \'{tradingday}\''
                cursor.execute(stmt)
                for row in cursor:
                    orderId.append(row['orderId'])
                    price.append(row['price'])
                    sliceAvgPrice.append(row['sliceAvgPrice'])
                    cumQty.append(row['cumQty'])
                    category.append(row['category'])
                    exDestination.append(row['exDestination'])
                    symbol.append(row['symbol'])
                    tradingdays.append(row['tradingDay'])
        data = pd.DataFrame({'orderId': orderId, 'price': price, 'sliceAvgPrice': sliceAvgPrice, 'cumQty': cumQty,
                             'category': category, 'exDestination': exDestination, 'symbol': symbol,
                             'tradingdays': tradingdays})

        data['cumQty'] = data['cumQty'].astype('int')
        data['price'] = data['price'].astype('float')
        data['sliceAvgPrice'] = data['sliceAvgPrice'].astype('float')
        return data

    def cal_num_turnover(self, df_exchange_order):
        num_exchange_orders = len(df_exchange_order)
        turnover_exchange_orders = round(sum(df_exchange_order['sliceAvgPrice'] * df_exchange_order['cumQty']) / 10000,
                                         2)
        return num_exchange_orders, turnover_exchange_orders

    def cal_aggressive_ratio(self, df_exchange_order):
        df_aggressive_exchange_order = df_exchange_order[df_exchange_order['category'] == 1]
        df_aggressive_turnover = sum(
            df_aggressive_exchange_order['sliceAvgPrice'] * df_aggressive_exchange_order['cumQty'])
        df_turnover = sum(df_exchange_order['sliceAvgPrice'] * df_exchange_order['cumQty'])
        aggressive_ratio = round(df_aggressive_turnover / df_turnover if df_turnover > 0 else 0, 4)
        return aggressive_ratio, 1 - aggressive_ratio

    def run(self, tradingDays, clientId):
        for tradingDay in tradingDays:
            self.logger.info(f'start calculator: {tradingDay}__{clientId}')
            # 所有订单
            all_clientOrders = self.get_all_clientOrder(tradingDay, clientId)
            trade_num = len(all_clientOrders)
            sum_turnover = round(sum(all_clientOrders['turnover'] / 10000), 2)
            all_slipage = 0 if sum_turnover == 0 else round(sum(
                all_clientOrders['slipageByVwap'] * all_clientOrders['turnover']) / sum_turnover, 2)

            all_clientOrders_sh = all_clientOrders[all_clientOrders['exDestination'] == 0]  # 上海所有订单
            all_clientOrders_sz = all_clientOrders[all_clientOrders['exDestination'] == 1]  # 深圳所有订单
            sh_trade_num = len(all_clientOrders_sh)
            sh_turnover = round(sum(all_clientOrders_sh['turnover'] / 10000), 2)
            sz_trade_num = len(all_clientOrders_sz)
            sz_turnover = round(sum(all_clientOrders_sz['turnover'] / 10000), 2)

            sh_slipage = 0 if sum(all_clientOrders_sh['turnover']) == 0 else round(sum(
                all_clientOrders_sh['slipageByVwap'] * all_clientOrders_sh['turnover']) / sum(
                all_clientOrders_sh['turnover']), 2)

            sz_slipage = 0 if sum(all_clientOrders_sz['turnover']) == 0 else round(sum(
                all_clientOrders_sz['slipageByVwap'] * all_clientOrders_sz['turnover']) / sum(
                all_clientOrders_sz['turnover']), 2)

            all_exchange_orders = self.join_ExchangeOrder_category(tradingDay, clientId)
            sh_exchange_orders = all_exchange_orders[all_exchange_orders['exDestination'] == 0]
            sz_exchange_orders = all_exchange_orders[all_exchange_orders['exDestination'] == 1]
            aggressive_sh_exchange_orders = sh_exchange_orders[sh_exchange_orders['category'] == 1]
            not_aggressive_sh_exchange_orders = sh_exchange_orders[sh_exchange_orders['category'] != 1]
            aggressive_sz_exchange_orders = sz_exchange_orders[sz_exchange_orders['category'] == 1]
            not_aggressive_sz_exchange_orders = sz_exchange_orders[sz_exchange_orders['category'] != 1]

            num_aggressive_sh_exchange_orders, turnover_aggressive_sh_exchange_orders = self.cal_num_turnover(
                aggressive_sh_exchange_orders)
            num_not_aggressive_sh_exchange_orders, turnover_not_aggressive_sh_exchange_orders = self.cal_num_turnover(
                not_aggressive_sh_exchange_orders)
            num_aggressive_sz_exchange_orders, turnover_aggressive_sz_exchange_orders = self.cal_num_turnover(
                aggressive_sz_exchange_orders)
            num_not_aggressive_sz_exchange_orders, turnover_not_aggressive_sz_exchange_orders = self.cal_num_turnover(
                not_aggressive_sz_exchange_orders)

            sh_aggressive_ratio, sh_not_aggressive_ratio = self.cal_aggressive_ratio(sh_exchange_orders)
            sz_aggressive_ratio, sz_not_aggressive_ratio = self.cal_aggressive_ratio(sz_exchange_orders)

            self.df_result.loc[tradingDay, 'trade_num'] = trade_num
            self.df_result.loc[tradingDay, 'turnover'] = sum_turnover
            self.df_result.loc[tradingDay, 'VWAPbps'] = all_slipage
            self.df_result.loc[tradingDay, 'sh_trade_num'] = sh_trade_num
            self.df_result.loc[tradingDay, 'sh_turnover'] = sh_turnover
            self.df_result.loc[tradingDay, 'sh_VWAPbps'] = sh_slipage
            self.df_result.loc[tradingDay, 'sz_trade_num'] = sz_trade_num
            self.df_result.loc[tradingDay, 'sz_turnover'] = sz_turnover
            self.df_result.loc[tradingDay, 'sz_VWAPbps'] = sz_slipage
            self.df_result.loc[tradingDay, 'sh_aggressive_turnover'] = turnover_aggressive_sh_exchange_orders
            self.df_result.loc[tradingDay, 'sh_aggressive_num'] = num_aggressive_sh_exchange_orders
            self.df_result.loc[tradingDay, 'sz_aggressive_turnover'] = turnover_aggressive_sz_exchange_orders
            self.df_result.loc[tradingDay, 'sz_aggressive_num'] = num_aggressive_sz_exchange_orders
            self.df_result.loc[tradingDay, 'sh_not_aggressive_turnover'] = turnover_not_aggressive_sh_exchange_orders
            self.df_result.loc[tradingDay, 'sh_not_aggressive_num'] = num_not_aggressive_sh_exchange_orders
            self.df_result.loc[tradingDay, 'sz_not_aggressive_turnover'] = turnover_not_aggressive_sz_exchange_orders
            self.df_result.loc[tradingDay, 'sz_not_aggressive_num'] = num_not_aggressive_sz_exchange_orders

            self.df_result.loc[tradingDay, 'sh_aggressive_ratio'] = sh_aggressive_ratio
            self.df_result.loc[tradingDay, 'sh_not_aggressive_ratio'] = sh_not_aggressive_ratio
            self.df_result.loc[tradingDay, 'sz_aggressive_ratio'] = sz_aggressive_ratio
            self.df_result.loc[tradingDay, 'sz_not_aggressive_ratio'] = sz_not_aggressive_ratio

        fileName = f'stat_{start}_{end}_({clientId}).xlsx'
        pathCsv = os.path.join(f'Data/{fileName}')

        ExcelHelper.createExcel(pathCsv)
        ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=self.df_result,
                                       header=True, sheet_name=clientId)
        ExcelHelper.removeSheet(pathCsv, 'Sheet')
        self.logger.info(f'calculator: {tradingDay}__{clientId} successfully')


if __name__ == '__main__':
    start = '20201201'
    end = '20210107'
    clientId = 'Cld_TRX_5001008'
    reporter = StatTransEffect(start, end, clientId)
