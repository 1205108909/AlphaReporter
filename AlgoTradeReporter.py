#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : AlgoTradeReporter.py 
@Time : 2021/12/6 8:52 
@FileDescription:客户本地部署时发送邮件
"""

import datetime
import os
import sys

import h5py
import numpy as np
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
from enum import Enum


class SendMode(Enum):
    clientId = 1
    accountId = 2
    clientId_accountId = 3


class AlgoTradeReporter(object):
    def __init__(self, start, end):
        self.logger = Log.get_logger(__name__)
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)
        self.email = EmailHelper.instance()

        self.run(tradingdays)

    def get_connection(self):
        try:
            self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
            return self.conn
        except pymssql.OperationalError as e:
            print(e)

    def get_clientOrder(self, tradingday, send_mode, clientId, accountId):
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
                if send_mode == SendMode.clientId:
                    stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId like \'{clientId}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
                elif send_mode == SendMode.accountId:
                    stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and accountId like \'{accountId}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
                elif send_mode == SendMode.clientId_accountId:
                    stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId like \'{clientId}\' and accountId like \'{accountId}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
                self.logger.info(stmt)
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


    def get_exchangeOrder(self, tradingday, send_mode, clientId, accountId):
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
                if send_mode == SendMode.clientId:
                    stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.clientId like \'{clientId}\' AND b.algo <> 'POV' AND b.algo <> 'PEGGING'"
                elif send_mode == SendMode.accountId:
                    stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.accountId like \'{accountId}\' AND b.algo <> 'POV' AND b.algo <> 'PEGGING'"
                elif send_mode == SendMode.clientId_accountId:
                    stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.clientId like \'{clientId}\' AND b.accountId like \'{accountId}\' AND b.algo <> 'POV' AND b.algo <> 'PEGGING'"
                self.logger.info(stmt)
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


    def get_all_receiveList(self):
        accountIds = []
        clientIds = []
        sendModes = []
        clientNames = []
        emails = []
        repsentEmails = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"select * from ClientsForPyLocal"

                self.logger.info(stmt)
                cursor.execute(stmt)
                for row in cursor:
                    if row['sendToClient'] == 'N':
                        continue
                    accountIds.append(row['accountId'])
                    clientIds.append(row['clientId'])
                    sendModes.append(row['sendMode'])
                    clientNames.append(row['clientName'])
                    emails.append(row['email'])
                    repsentEmails.append(row['repsentEmail'])
        data = pd.DataFrame(
            {'accountId': accountIds, 'clientId': clientIds, 'sendMode': sendModes, 'clientName': clientNames,
             'to_receiver': emails, 'cc_receiver': repsentEmails})
        return data

    def run(self, tradingDays):
        df_clients = self.get_all_receiveList()
        df_only_clientId = df_clients[df_clients['sendMode'] == 1]
        df_only_accountId = df_clients[df_clients['sendMode'] == 2]
        df_clientId_accountId = df_clients[df_clients['sendMode'] == 3]

        for tradingDay in tradingDays:
            self.dir_data = os.path.join(f'Data/AlgoTradeReporter/{tradingDay}')
            if not os.path.exists(self.dir_data):
                os.makedirs(self.dir_data)

            self.main_cal(df_only_clientId, ['clientId'], SendMode.clientId, tradingDay)
            self.main_cal(df_only_accountId, ['accountId'], SendMode.accountId, tradingDay)
            self.main_cal(df_clientId_accountId, ['clientId', 'accountId'], SendMode.clientId_accountId, tradingDay)

    def main_cal(self, df_clients, subset, send_mode, tradingDay):
        df_clients.drop_duplicates(subset=subset, keep='first', inplace=True)
        for index, account in df_clients.iterrows():
            clientId = account['clientId']
            accountId = account['accountId']
            clientName = account['clientName']
            if send_mode == SendMode.clientId:
                showId = clientId
            elif send_mode == SendMode.accountId:
                showId = accountId
            elif send_mode == SendMode.clientId_accountId:
                showId = accountId
            self.logger.info(f'start calculator: {tradingDay}__{showId}')
            df_clientOrder = self.get_clientOrder(tradingday=tradingDay, send_mode=send_mode,
                                                  clientId=clientId,
                                                  accountId=accountId)

            df_exchange_order = self.get_exchangeOrder(tradingday=tradingDay, send_mode=send_mode,
                                                       clientId=clientId,
                                                       accountId=accountId)

            cancelRatio = sum(df_exchange_order['qty'] - df_exchange_order['cumQty']) / sum(
                df_exchange_order['qty'])
            clientOrderCount = len(df_clientOrder)
            sumAmt = sum(df_clientOrder['turnover'])
            bps = sum(df_clientOrder['slipageByVWAP'] * df_clientOrder['turnover']) / sum(
                df_clientOrder['turnover'])

            df_summary = pd.DataFrame(
                {"撤单率": f"{round(cancelRatio * 100, 2)}%", "母单个数": clientOrderCount,
                 "成交额(万元)": round(sumAmt / 10000, 2),
                 "成交额加权滑点差(bps,去除POV、PEGGING)": bps}, index=[1])

            fileName = f'AlgoTradeReporter_{tradingDay}_({showId}).xlsx'
            pathCsv = os.path.join(self.dir_data, fileName)
            ExcelHelper.createExcel(pathCsv)
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_summary, header=True,
                                           sheet_name='algoSummary', sep_key='all_name')
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_clientOrder,
                                           header=True, sheet_name='algoClientOrder', sep_key='all_name')
            ExcelHelper.removeSheet(pathCsv, 'Sheet')

            self.email.add_email_content(f'华创算法：AlgoTradeReporter_{tradingDay}_({accountId})交易报告，请查收')
            subject = f'华创算法：AlgoTradeReporter:{clientName}({accountId})_{tradingDay}'
            self.email.send_email_file(pathCsv, fileName, to_receiver=account.to_receiver.split(';'),
                                       cc_receiver=account.cc_receiver.split(';'), subject=subject)
            self.email.content = ''
            self.logger.info(f'calculator: {tradingDay}__{accountId} successfully')


if __name__ == '__main__':
    start = sys.argv[1]
    end = sys.argv[2]
    reporter = AlgoTradeReporter(start, end)
