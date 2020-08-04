#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : app.py 
@Time : 2020/8/3 9:11 
"""

import os
import sys
from configparser import RawConfigParser
import pymssql
import Log
import pandas as pd
from Constants import Constants

# 显示所有列
from DataSender.EmailHelper import EmailHelper

pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class App(object):

    def __init__(self, start, end):
        self.Log = Log.get_logger(__name__)
        self.email = EmailHelper()

        cfg = RawConfigParser()
        cfg.read('config.ini')
        self.ClientID = cfg.get('ClientID', 'id')
        self.ClientIDs = list(self.ClientID.split(';'))
        self.AccountID = cfg.get('AccountID', 'id')
        self.AccountIDs = list(self.AccountID.split(';'))

        self.server = cfg.get('AlgoTradeReport', 'server')
        self.user = cfg.get('AlgoTradeReport', 'user')
        self.password = cfg.get('AlgoTradeReport', 'password')
        self.database = cfg.get('AlgoTradeReport', 'database')

        self.localOutputPath = cfg.get('OutputPath', 'path')
        self.conn = None

        self.dfSummary = pd.DataFrame()
        for clientId in self.ClientIDs:
            dfSignalEffect = self.signalEffect(start, end, clientId, 0)
            dfTurnoverRatio = self.calTurnOverRatio(start, end, clientId, 0)
            dfTurnoverRatio.columns = dfTurnoverRatio.columns.map(lambda x: Constants.PlacementCategoryDict[x])
            dfOneId = dfSignalEffect.merge(dfTurnoverRatio, left_on='type', right_on=dfTurnoverRatio.index, how='left')
            self.dfSummary = pd.concat([self.dfSummary, dfOneId])

        for accountID in self.AccountIDs:
            dfSignalEffect = self.signalEffect(start, end, accountID, 1)
            dfTurnoverRatio = self.calTurnOverRatio(start, end, accountID, 1)
            dfTurnoverRatio.columns = dfTurnoverRatio.columns.map(lambda x: Constants.PlacementCategoryDict[x])
            dfOneId = dfSignalEffect.merge(dfTurnoverRatio, left_on='type', right_on=dfTurnoverRatio.index, how='left')
            self.dfSummary = pd.concat([self.dfSummary, dfOneId])

        self.dfSummary = self.dfSummary[['Id', 'type', 'turnover', 'spread', 'Aggressive', 'Passive', 'UltraPassive']]
        if not os.path.exists(self.localOutputPath):
            os.mkdir(self.localOutputPath)
        pathCsv = os.path.join(self.localOutputPath, 'SignalEffect_' + start + '_' + end + '.csv')
        fileName = 'SignalEffect_' + start + '_' + end + '.csv'
        self.dfSummary.to_csv(pathCsv, encoding="utf_8_sig")
        self.email.sendEmail(pathCsv, fileName, start, end)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                print(e)

    def signalEffect(self, start, end, Id, isClient):
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

        df = pd.DataFrame({'Id': clientIds, 'type': type, 'turnover': turnover, 'spread': spread})
        return df

    def calTurnOverRatio(self, start, end, clientId, isClient):
        category = []
        signalType = []
        fillRatio = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_StatTurnoverRatio'
                cursor.callproc(proc, (clientId, start, end, isClient))
                for row in cursor:
                    category.append(row['category'])
                    signalType.append(row['signalType'])
                    fillRatio.append(row['fillRatio'])

        df = pd.DataFrame({'category': category, 'signalType': signalType, 'fillRatio': fillRatio})
        df = df.pivot(index='signalType', columns='category', values='fillRatio')
        return df


if __name__ == '__main__':
    start = sys.argv[1]
    end = sys.argv[2]
    app = App(start, end)
