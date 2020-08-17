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
from DataSender.ExcelHelper import ExcelHelper

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

        if not os.path.exists(self.localOutputPath):
            os.mkdir(self.localOutputPath)
        pathCsv = os.path.join(self.localOutputPath, 'SignalEffect_' + start + '_' + end + '.xlsx')
        fileName = 'SignalEffect_' + start + '_' + end + '.xlsx'
        ExcelHelper.createExcel(pathCsv)
        for clientId in self.ClientIDs:
            if len(clientId) == 0:
                continue
            self.calSignalEffect(clientId, pathCsv, end, start, 0)
        for accountId in self.AccountIDs:
            if len(accountId) == 0:
                continue
            self.calSignalEffect(accountId, pathCsv, end, start, 1)
        ExcelHelper.removeSheet(pathCsv, 'Sheet')
        self.email.sendEmail(pathCsv, fileName, start, end)

    def calSignalEffect(self, clientId, pathCsv, start, end, isclinet):
        # 1.信号效果
        dfSignalEffect = self._statSignalEffect(start, end, clientId, isclinet)
        # 2.订单成交率
        dfTurnoverRatio, dfTurnoverRatiowithQty = self._statTurnOverRatio(start, end, clientId, isclinet)
        dfTurnoverRatio.columns = dfTurnoverRatio.columns.map(lambda x: Constants.PlacementCategoryDict[x])
        dfTurnoverRatiowithQty['category'] = dfTurnoverRatiowithQty['category'].map(
            lambda x: Constants.PlacementCategoryDict[x])
        # 2.1 合并信号效果与订单成交率
        dfSignalEffect = dfSignalEffect.merge(dfTurnoverRatio, left_on='type', right_on=dfTurnoverRatio.index,
                                              how='left')
        dfSignalEffect = dfSignalEffect[
            ['Id', 'type', 'turnover', 'slipage', 'Aggressive', 'Passive', 'UltraPassive']]
        dfSignalEffect['type'] = dfSignalEffect['type'].map(lambda x: Constants.SingalType2Chn[x])
        # 3. passive/ultraPassive 比例
        try:
            dfRatio = self._statRelativeRate(clientId, dfTurnoverRatiowithQty)
        except Exception as e:
            dfRatio = None
            self.Log.error(e)
            self.Log.error('passive / ultraPassive比例')

        # 4.客户slipage排名
        dfSlipageInBps = self._statSlipageInBps(start, end, clientId, isclinet)
        dfSlipageInBpsWorse20 = dfSlipageInBps.head(20)
        dfSlipageInBpsBetter20 = dfSlipageInBps.tail(20)
        dfSlipageInBpsBetter20.sort_values(by='slipageInBps', axis=0, ascending=False, inplace=True)
        dfSlipageInBpsBetter20 = dfSlipageInBpsBetter20.reset_index(drop=True)

        if not dfSignalEffect is None:
            ExcelHelper.Append_df_to_excel(pathCsv, dfSignalEffect, header=True, sheet_name=clientId)
        if not dfRatio is None:
            ExcelHelper.Append_df_to_excel(pathCsv, dfRatio, header=True, interval=4, sheet_name=clientId)
        if not dfSlipageInBpsWorse20 is None:
            ExcelHelper.Append_df_to_excel(pathCsv, dfSlipageInBpsWorse20, header=True, interval=4, sheet_name=clientId)
        if not dfSlipageInBpsBetter20 is None:
            ExcelHelper.Append_df_to_excel(pathCsv, dfSlipageInBpsBetter20, header=True, interval=4,
                                           sheet_name=clientId)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                print(e)

    def _statSignalEffect(self, start, end, Id, isClient):
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
        dfpivot = df.pivot(index='signalType', columns='category', values='fillRatio')
        return dfpivot, df

    def _statRelativeRate(self, id, df):
        ids = []
        type = []
        normalRate = []
        reveRate = []

        ids.append(id)
        type.append('passive/ultraPassive')
        cumQtyNormalPassive = (df[(df['signalType'] == 'Normal') & (df['category'] == 'Passive')]['cumQty']).values[0][
            0]
        cumQtyNormalUltraPassive = \
            (df[(df['signalType'] == 'Normal') & (df['category'] == 'UltraPassive')]['cumQty']).values[0][0]
        normalRate.append(round(cumQtyNormalPassive / cumQtyNormalUltraPassive, 4))

        cumQtyRevePassive = (df[(df['signalType'] == 'Reverse') & (df['category'] == 'Passive')]['cumQty']).values[0][0]
        cumQtyReveUltraPassive = \
            (df[(df['signalType'] == 'Reverse') & (df['category'] == 'UltraPassive')]['cumQty']).values[0][0]
        reveRate.append(round(cumQtyRevePassive / cumQtyReveUltraPassive, 4))

        df = pd.DataFrame({'id': ids, 'description': type, 'normalRate': normalRate, 'reverseRate': reveRate})

        return df

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
                sql = f"SELECT symbol,side, orderQty, slipageInBps, effectiveTime, expireTime FROM ClientOrderView WHERE {idtype} LIKE \'{Id}\' AND tradingDay >= \'{start}\' AND tradingDay <= \'{end}\' AND avgPrice * cumQty > 500000 ORDER BY slipageInBps"
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
        return df


if __name__ == '__main__':
    start = sys.argv[1]
    end = sys.argv[2]
    app = App(start, end)
