#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: JydbSource.py
@time: 2018/7/12 17:46
"""
from configparser import ConfigParser
import pymssql
import pandas as pd
import sys

import Log
from numpy import NaN

from DataService import JydbConst

sys.path.append('..')


class JydbSource(object):

    def __init__(self):
        self.server = ""
        self.user = ""
        self.password = ""
        self.database = ""
        self.initialize()
        self.conn = None
        self.logger = Log.get_logger(__name__)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                self.logger.error(e)

    def get_tradingday(self, start, end):
        """
        返回交易日序列
        :param start: '20150101'
        :param end: '20150130'
        :return: tradingDay: ['20150101', '20150101']
        """
        tradingDay = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradingDate'
                cursor.callproc(proc, (start, end))
                for row in cursor:
                    tradingDay.append(row['TradingDate'])

        return tradingDay

    def get_month_end_tradingday(self, start, end):
        """
        返回月末交易日
        :param start: '20150101'
        :param end: '20150130'
        :return: tradingday: ['20150101', '20150101'[
        """
        tradingDay = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT CONVERT(VARCHAR(8), TradingDate, 112) as TradingDate FROM QT_TradingDayNew WHERE IfTradingDay=1 AND TradingDate BETWEEN \'%s\' AND \'%s\' AND SecuMarket In (83, 90) and IfMonthEnd=1 ORDER BY TradingDate' % (
                    start, end)

                cursor.execute(stmt)
                for row in cursor:
                    tradingDay.append(row['TradingDate'])
        return tradingDay

    def initialize(self):
        cfg = ConfigParser()
        cfg.read('config.ini')
        self.server = cfg.get('Jydb', 'server')
        self.user = cfg.get('Jydb', 'user')
        self.password = cfg.get('Jydb', 'password')
        self.database = cfg.get('Jydb', 'database')

    def get_tradableList(self, tradingDay):
        """
        返回交易日可交易证券列表
        :param tradingDay: '20150101’
        :return: ['600000.sh', '000001.sz']
        """

        stockId = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableSecurity'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] == 'BDC' or row['sType'] == 'FTR' or row['isTradable'] == 0:
                        continue
                    stockId.append(row['stockId'])
        return stockId

    def get_tradableItem(self, tradingDay):
        """
        返回交易日前所有上市过的证券的列表用字段标明是否可交易
        :param tradingDay: '20150101'
        :return: pd.DataFrame
        """
        stockId = []
        stockname = []
        sType = []
        isTradable = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                # proc = 'spu_GetTradableSecurity'
                proc = 'spu_GetTradableStock'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    # if row['sType'] == 'BDC' or row['sType'] == 'FTR':
                    if row['sType'] != 'EQA':
                        continue
                    stockId.append(row['stockId'])
                    stockname.append(row['stockName'])
                    # stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    sType.append(row['sType'])
                    isTradable.append(row['isTradable'])

        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'isTradable': isTradable})
        return df

    def get_universe(self, tradingDay):
        """
        返回在交易日之前的全部证券基础信息
        :param tradingDay: '20150101'
        :return: df : pd.DataFrame
        """
        stockId = []
        stockname = []
        sType = []
        listDate = []
        delistDate = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableSecurity'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] == 'BDC' or row['sType'] == 'FTR':
                        continue
                    stockId.append(row['stockId'])
                    stockname.append(row['stockName'])
                    # stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    sType.append(row['sType'])
                    listDate.append(row['listDate'])
                    delistDate.append(row['delistDate'])
        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'ListDate': listDate,
             'DelistDate': delistDate})
        return df

    def get_stock_universe(self, tradingDay):
        """
        返回在交易日之前的全部证券基础信息
        :param tradingDay: '20150101'
        :return: df : pd.DataFrame
                    Symbol ChineseName Type ListDate DelistDate
                    Type - 'EQA'
        """
        stockId = []
        stockname = []
        sType = []
        listDate = []
        delistDate = []
        isTradable = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableStock'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] != 'EQA':
                        continue
                    stockId.append(row['stockId'])
                    # stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    stockname.append(row['stockName'])
                    sType.append(row['sType'])
                    listDate.append(row['listDate'])
                    delistDate.append(row['delistDate'])
                    isTradable.append(row['isTradable'])
        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'ListDate': listDate,
             'DelistDate': delistDate, 'isTradable': isTradable})
        return df

    def getDayBarByDay(self, tradingday):
        """
        获得日线数据
        :param tradingDay: '20150101'
        :return:
        """

        symbol = []
        preClose = []
        open_p = []
        close = []
        high = []
        low = []
        volume = []
        turnover = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT CASE SecuMarket when 83 then SecuCode+\'.sh\' when 90 then SecuCode+\'.sz\' End as Symbol,TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue FROM LC_STIBDailyQuote t1 LEFT JOIN SecuMain t2 ON t1.InnerCode = t2.InnerCode where SecuMarket in (83,90) and SecuCategory = 1 and TradingDay = \'%s\' UNION ALL SELECT CASE SecuMarket when 83 then SecuCode+\'.sh\' when 90 then SecuCode+\'.sz\' End as Symbol,TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue FROM QT_DailyQuote t1 LEFT JOIN SecuMain t2 ON t1.InnerCode = t2.InnerCode where SecuMarket in (83,90) and SecuCategory = 1 and TradingDay = \'%s\'' % (
                    tradingday, tradingday)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'TradingDay'       '20150101'
                # 'PrevClosePrice'    12.60
                # 'OpenPrice'    12.60
                # 'HighPrice'    12.60
                # 'LowPrice'    12.60
                # 'ClosePrice'    12.60
                # 'TurnoverVolume'    31323053
                # 'TurnoverValue'    398614966
                for row in cursor:
                    symbol.append(row['Symbol'])
                    preClose.append(row['PrevClosePrice'])
                    open_p.append(row['OpenPrice'])
                    close.append(row['ClosePrice'])
                    high.append(row['HighPrice'])
                    low.append(row['LowPrice'])
                    volume.append(row['TurnoverVolume'])
                    turnover.append(row['TurnoverValue'])
        df = pd.DataFrame(
            {'Symbol': symbol, 'Open': open_p, 'Close': close, 'High': high,
             'Low': low, 'Volume': volume, 'Turnover': turnover, 'PreClose': preClose})
        return df

    def getDayBar(self, symbol, start, end):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        innerCode = self.get_stockInnerCode(symbol)
        if innerCode is None:
            return pd.DataFrame()
        tradingDay = []
        preClose = []
        open_p = []
        close = []
        high = []
        low = []
        volume = []
        turnover = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                table = 'QT_DailyQuote'
                if symbol.startswith('688'):
                    table = 'LC_STIBDailyQuote'
                stmt = 'SELECT TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue FROM %s WHERE InnerCode = %d AND TradingDay>=\'%s\' AND TradingDay <=\'%s\' ORDER BY TradingDay' % (
                    table, innerCode, start, end)
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s %s-%s getDayBar got wrong' % (symbol, start, end))
                    return pd.DataFrame()

                for row in cursor:
                    tradingDay.append(row['TradingDay'].strftime('%Y%m%d'))
                    preClose.append(row['PrevClosePrice'])
                    open_p.append(row['OpenPrice'])
                    close.append(row['ClosePrice'])
                    high.append(row['HighPrice'])
                    low.append(row['LowPrice'])
                    volume.append(row['TurnoverVolume'])
                    turnover.append(row['TurnoverValue'])
        df = pd.DataFrame(
            {'Date': tradingDay, 'Open': open_p, 'Close': close, 'High': high,
             'Low': low, 'Volume': volume, 'Turnover': turnover, 'PreClose': preClose})
        return df

    def get_stockInnerCode(self, symbol):
        """
        根据交易所代码返回inner_code序列
        :param symbol: '000001.sz'
        :return: innerCode: 3
        """
        result = None
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT InnerCode, SecuCode FROM vwu_SecuMain WHERE SecuCode = \'%s\' AND SecuMarket in (83, 90) AND SecuCategory=1' % symbol[
                                                                                                                                              : 6]

                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'secuCode'       000001
                # 'InnerCode'    'sz'/'sh'
                for row in cursor:
                    result = row['InnerCode']
        return result

    def getDividend(self, start, end):
        """
        获得除权除息数据
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        tradingDay = pd.DataFrame({'ExdiviDate': self.get_tradingday('19900101', end)})
        symbol = []
        exdividate = []
        adjustingFactor = []
        adjustingConst = []
        ratioAdjustingFactor = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select Case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as Symbol, t1.ExdiviDate, 1 as AdjustingFactor, 0 AS AdjustingConst, t1.AdjustingFactor as RatioAdjustingFactor from LC_STIBAdjustingFactor t1 left join SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.ExDiviDate >= \'%s\' and t1.ExDiviDate <= \'%s\' and t2.SecuCategory = 1 UNION ALL select Case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as Symbol, t1.ExdiviDate, t1.AdjustingFactor, t1.AdjustingConst, t1.RatioAdjustingFactor from QT_AdjustingFactor t1 left join SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.ExDiviDate >= \'%s\' and t1.ExDiviDate <= \'%s\' and t2.SecuCategory = 1' % (
                    '19900101', end, '19900101', end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'Symbol'       '600000.sh'
                # 'ExdiviDate'    '20150101'
                # 'AdjustingFactor'    1.0
                # 'AdjustConst'    1.0
                # 'RatioAdjustingFactor'    1.0
                for row in cursor:
                    symbol.append(row['Symbol'])
                    exdividate.append(row['ExdiviDate'].strftime('%Y%m%d'))
                    adjustingFactor.append(float(row['AdjustingFactor']))
                    adjustingConst.append(float(row['AdjustingConst']))
                    ratioAdjustingFactor.append(
                        float(row['RatioAdjustingFactor']) if row['RatioAdjustingFactor'] is not None else None)

        df = pd.DataFrame(
            {'Symbol': symbol, 'ExdiviDate': exdividate, 'AdjustingFactor': adjustingFactor,
             'AdjustingConst': adjustingConst,
             'RatioAdjustingFactor': ratioAdjustingFactor})

        result = pd.DataFrame(columns=['Symbol', 'ExdiviDate', 'Stock', 'Cash', 'RatioAdjustingFactor'])
        symbols = pd.unique(df['Symbol'])
        for s in symbols:
            data = pd.merge(tradingDay, df[df['Symbol'] == s], how='left', on=['ExdiviDate'])
            data.fillna(method='ffill', inplace=True)
            data.fillna(method='bfill', inplace=True)
            data['Stock'] = data['AdjustingFactor'] / data['AdjustingFactor'].shift(1) - 1
            data['Cash'] = data['AdjustingConst'].diff() / data['AdjustingFactor'].shift(1)
            data.drop([0], inplace=True)
            data = data.loc[(data['ExdiviDate'] >= start) & (data['ExdiviDate'] <= end)]
            result = result.append(data, ignore_index=True)
        result = result[['Symbol', 'ExdiviDate', 'Stock', 'Cash', 'RatioAdjustingFactor']]
        # result['Stock'] = result['AdjustingFactor'] / result['AdjustingFactor'].shift(1) - 1
        # result['Cash'] = result['AdjustingConst'].diff() / result['AdjustingFactor'].shift(1)
        # del result['AdjustingFactor'], result['AdjustingConst']
        return result

    def getIndexUniverse(self, tradingDay):
        """
        返回申万指数与上海深圳中证指数基本信息
        :param tradingDay: '20150101'
        :return: pd.DataFrame()
                     'Symbol', 'Name', 'Market', 'Publisher', 'Category', 'ListDate'
        """
        symbol = []
        name = []
        publisher = []
        list_date = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                if tradingDay >= '20140221':
                    stmt = 'select t2.InnerCode, t2.SecuCode as Symbol, t2.SecuAbbr as Name , t1.PubOrgName as Publisher, t1.PubDate as ListDate from LC_IndexBasicInfo t1 LEFT JOIN SecuMain t2 on t1.IndexCode = t2.InnerCode where t2.InnerCode in (1, 46, 3145, 4978, 4982, 39144, 11089, 7542, 1055, 1059, 5375, 5376, 5377, 5378, 5379, 5382, 5385, 5386, 5387, 5388, 5389, 5390, 5391, 5392, 5394, 5395, 5397, 32616, 32617, 32618, 32619, 32620, 32621, 32622, 32623, 32624, 32625, 32626)'
                else:
                    stmt = 'select t2.InnerCode, t2.SecuCode as Symbol, t2.SecuAbbr as Name , t1.PubOrgName as Publisher, t1.PubDate as ListDate from LC_IndexBasicInfo t1 LEFT JOIN SecuMain t2 on t1.IndexCode = t2.InnerCode where t2.InnerCode in (1, 46, 3145, 4978, 4982, 39144, 11089, 7542, 1055, 1059, 5375, 5376, 5377, 5378, 5379, 5380, 5381, 5382, 5383, 5384, 5385, 5386, 5387, 5388, 5389, 5390, 5391, 5392, 5393, 5394, 5395, 5396, 5397)'
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['Symbol'])
                    name.append(row['Name'])
                    # name.append(row['Name'].encode('latin-1').decode('gbk'))
                    # publisher.append(row['Publisher'].encode('latin-1').decode('gbk'))
                    publisher.append(row['Publisher'])
                    list_date.append(row['ListDate'].strftime('%Y%m%d'))

        df = pd.DataFrame({'Symbol': symbol, 'Name': name, 'Publisher': publisher, 'ListDate': list_date})
        return df

    def getIndexFutureTradable(self, tradingday):
        stockId = []
        stockname = []
        sType = []
        listDate = []
        delistDate = []
        isTradable = []
        exchangeCode = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetIndexFutureUniverse'
                cursor.callproc(proc, (tradingday,))
                for row in cursor:
                    stockId.append(row['stockId'])
                    # stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    stockname.append(row['stockName'])
                    sType.append(row['sType'])
                    listDate.append(row['listDate'])
                    delistDate.append(row['delistDate'])
                    isTradable.append(row['isTradable'])
                    exchangeCode.append(row['secuMarket'])
        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'ListDate': listDate,
             'DelistDate': delistDate, 'isTradable': isTradable, 'exchangeCode': exchangeCode})
        return df

    def getIndexDayBar(self, symbol, start, end):
        """
        返回给定指数起止日期日线数据
        :param symbol: '399001'
        :param start: '20180901'
        :param end: '20180914'
        :return:
        """
        date = []
        open_p = []
        close_p = []
        high = []
        low = []
        pre_close = []
        volume = []
        turnvoer = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select InnerCode from SecuMain where SecuCategory = 4 and SecuMarket in (83, 90) and SecuCode = \'%s\'' % symbol
                try:
                    cursor.execute(stmt)
                    for row in cursor:
                        innerCode = row['InnerCode']
                    stmt = 'select TradingDay as Date, OpenPrice as OpenP, HighPrice as High, LowPrice as Low, ClosePrice as CloseP, TurnoverVolume as Volume, TurnoverValue as Turnover, PrevClosePrice as PreClose from QT_IndexQuote where Innercode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\' order by TradingDay' % (
                        innerCode, start, end)

                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s %s-%s getIndexDayBar got wrong' % (symbol, start, end))
                    return pd.DataFrame()
                for row in cursor:
                    date.append(row['Date'].strftime('%Y%m%d'))
                    open_p.append(row['OpenP'])
                    close_p.append(row['CloseP'])
                    high.append(row['High'])
                    low.append(row['Low'])
                    pre_close.append(row['PreClose'])
                    volume.append(row['Volume'])
                    turnvoer.append(row['Turnover'])
        data = pd.DataFrame(
            {'Date': date, 'Open': open_p, 'Close': close_p, 'High': high, 'Low': low, 'PreClose': pre_close,
             'Volume': volume, 'Turnover': turnvoer})
        return data

    def getIndexFutureDayBarByDay(self, tradingday):
        """
        返回期货日线数据
        :param symbol: '399001'
        :param start: '20180901'
        :param end: '20180914'
        :return:
        """
        contractInnerCode = []
        contractName = []
        date = []
        open_p = []
        close_p = []
        high = []
        low = []
        pre_close = []
        pre_settle = []
        volume = []
        turnover = []
        settle = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select t2.ContractInnerCode as ContractInnerCode,t2.ContractName as ContractName,t1.TradingDay as Date, OpenPrice as OpenP, ClosePrice as CloseP, HighPrice as High, LowPrice as Low, TurnoverVolume as Volume, TurnoverValue as Turnover, PrevClosePrice as PreClose, PrevSettlePrice as PreSettle, SettlePrice from Fut_TradingQuote t1 LEFT JOIN Fut_ContractMain t2 On t1.ContractInnerCode = t2.ContractInnerCode WHERE t1.TradingDay >= \'%s\' and t1.TradingDay <= \'%s\' and t2.ContractType in (3,4) and t2.EffectiveDate <= \'%s\' AND t2.LastTradingDate >= \'%s\'' % (
                    tradingday, tradingday, tradingday, tradingday)
                try:
                    cursor.execute(stmt)

                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s getIndexFutureDayBarByDay got wrong' % (tradingday))
                    return pd.DataFrame()
                for row in cursor:
                    contractInnerCode.append(row['ContractInnerCode'])
                    contractName.append(row['ContractName'])
                    date.append(row['Date'].strftime('%Y%m%d'))
                    open_p.append(row['OpenP'])
                    close_p.append(row['CloseP'])
                    high.append(row['High'])
                    low.append(row['Low'])
                    pre_close.append(row['PreClose'])
                    pre_settle.append(row['PreSettle'])
                    volume.append(row['Volume'])
                    turnover.append(row['Turnover'])
                    settle.append(row['SettlePrice'])
        data = pd.DataFrame(
            {'ContractInnerCode': contractInnerCode, 'ContractName': contractName, 'Date': date, 'Open': open_p,
             'Close': close_p, 'High': high, 'Low': low, 'PreClose': pre_close,
             'PreSettle': pre_settle,
             'Volume': volume, 'Turnover': turnover, 'Settle': settle})
        return data

    def getIndexFutureDayBar(self, symbol, start, end):
        """
        返回期货日线数据
        :param symbol: '399001'
        :param start: '20180901'
        :param end: '20180914'
        :return:
        """
        date = []
        open_p = []
        close_p = []
        high = []
        low = []
        pre_close = []
        pre_settle = []
        volume = []
        turnvoer = []
        settle = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select * from Fut_ContractMain where ContractCode =  \'%s\'' % symbol
                try:
                    cursor.execute(stmt)
                    for row in cursor:
                        innerCode = row['ContractInnerCode']
                    stmt = 'select TradingDay as Date, OpenPrice as OpenP, HighPrice as High, LowPrice as Low, ClosePrice as CloseP, TurnoverVolume as Volume, TurnoverValue as Turnover, PrevClosePrice as PreClose, PrevSettlePrice as PreSettle , SettlePrice from Fut_TradingQuote where ContractInnerCode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\' order by TradingDay' % (
                        innerCode, start, end)

                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s %s-%s getIndexFutureDayBar got wrong' % (symbol, start, end))
                    return pd.DataFrame()
                for row in cursor:
                    date.append(row['Date'].strftime('%Y%m%d'))
                    open_p.append(row['OpenP'])
                    close_p.append(row['CloseP'])
                    high.append(row['High'])
                    low.append(row['Low'])
                    pre_close.append(row['PreClose'])
                    pre_settle.append(row['PreSettle'])
                    volume.append(row['Volume'])
                    turnvoer.append(row['Turnover'])
                    settle.append(row['SettlePrice'])
        data = pd.DataFrame(
            {'Date': date, 'Open': open_p, 'Close': close_p, 'High': high, 'Low': low, 'PreClose': pre_close,
             'PreSettle': pre_settle,
             'Volume': volume, 'Turnover': turnvoer, 'SettlePrice': settle})

        return data

    def getIndexComponentWeight(self, index, traddingday):
        symbol = []
        weight = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select InnerCode from SecuMain where SecuCategory = 4 and SecuMarket in (83, 90) and SecuCode = \'%s\'' % index
                cursor.execute(stmt)
                for row in cursor:
                    innerCode = row['InnerCode']
                if index.startswith('801'):
                    stmt = 'select case t2.SecuMarket when 83 then t2.SecuCode+\'.sh\' when 90 then t2.SecuCode+\'.sz\' end as Symbol, t1.Weight from LC_SWSIndexCW t1 LEFT JOIN SecuMain t2 on t2.SecuCategory = 1 and t1.InnerCode = t2.InnerCode where IndexCode = %s and EndDate = \'%s\'' % (
                        innerCode, traddingday)
                else:
                    stmt = 'select case t2.SecuMarket when 83 then t2.SecuCode+\'.sh\' when 90 then t2.SecuCode+\'.sz\' end as Symbol, t1.Weight from LC_IndexComponentsWeight t1 LEFT JOIN SecuMain t2 on t2.SecuCategory = 1 and t1.InnerCode = t2.InnerCode where IndexCode = %s and EndDate = \'%s\'' % (
                        innerCode, traddingday)
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['Symbol'])
                    weight.append(row['Weight'])
        data = pd.DataFrame({'Symbol': symbol, 'Weight': weight})
        return data

    def getIndexWeight(self, index, tradingday):
        symbol = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select InnerCode from SecuMain where SecuCategory = 4 and SecuMarket in (83, 90) and SecuCode = \'%s\'' % index
                cursor.execute(stmt)
                for row in cursor:
                    innerCode = row['InnerCode']
                stmt = 'select case t2.SecuMarket when 83 then t2.SecuCode+\'.sh\' when 90 then t2.SecuCode+\'.sz\' end as Symbol from LC_IndexComponent t1 LEFT JOIN SecuMain t2 on t2.SecuCategory = 1 and t1.SecuInnerCode = t2.InnerCode where IndexInnerCode = %s and t2.ListedDate <= \'%s\' and t1.InDate <= \'%s\' and (OutDate > \'%s\' Or OutDate is null)' % (
                    innerCode, tradingday, tradingday, tradingday)
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['Symbol'])
        data = pd.DataFrame({'Symbol': symbol})
        data['Weight'] = None
        return data

    ## 未加入科创板
    def getBalanceSheet(self, start, end, symbol):
        """
        获得资产负债表
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                table = 'LC_BalanceSheetAll'
                if symbol.startswith('688'):
                    table = 'LC_STIBBalanceSheet'
                stmt = 'Select t1.InfoPublDate, t1.EndDate, CashEquivalents,ClientDeposit,TradingAssets,BillReceivable,DividendReceivable,BillAccReceivable,InterestReceivable,AccountReceivable,ContractualAssets,OtherReceivable,AdvancePayment,Inventories,BearerBiologicalAssets,DeferredExpense,HoldAndFSAssets,NonCurrentAssetIn1Year,OtherCurrentAssets,CAExceptionalItems,CAAdjustmentItems,TotalCurrentAssets,DebtInvestment,OthDebtInvestment,HoldToMaturityInvestments,HoldForSaleAssets,OthEquityInstrument,OthNonCurFinAssets,InvestmentProperty,LongtermEquityInvest,LongtermReceivableAccount,FixedAssets,ConstructionMaterials,ConstruInProcess,FixedAssetsLiquidation,BiologicalAssets,OilGasAssets,IntangibleAssets,SeatCosts,DevelopmentExpenditure,GoodWill,LongDeferredExpense,DeferredTaxAssets,OtherNonCurrentAssets,NCAExceptionalItems,NCAAdjustmentItems,TotalNonCurrentAssets,LoanAndAccountReceivables,SettlementProvi,ClientProvi,DepositInInterbank,RMetal,LendCapital,DerivativeAssets,BoughtSellbackAssets,LoanAndAdvance,InsuranceReceivables,ReceivableSubrogationFee,ReinsuranceReceivables,ReceivableUnearnedR,ReceivableClaimsR,ReceivableLifeR,ReceivableLTHealthR,InsurerImpawnLoan,FixedDeposit,RefundableDeposit,RefundableCapitalDeposit,IndependenceAccountAssets,OtherAssets,AExceptionalItems,AAdjustmentItems,TotalAssets,ShortTermLoan,ImpawnedLoan,TradingLiability,NotesPayable,AccountsPayable,NotAccountsPayable,ContractLiability,STBondsPayable,AdvanceReceipts,SalariesPayable,DividendPayable,TaxsPayable,InterestPayable,OtherPayable,AccruedExpense,DeferredProceeds,HoldAndFSLi,NonCurrentLiabilityIn1Year,OtherCurrentLiability,CLExceptionalItems,CLAdjustmentItems,TotalCurrentLiability,LongtermLoan,BondsPayable,LPreferStock,LPerpetualDebt,LongtermAccountPayable,LongSalariesPay,SpecificAccountPayable,EstimateLiability,DeferredTaxLiability,LongDeferIncome,OtherNonCurrentLiability,NCLExceptionalItems,NCLAdjustmentItems,TotalNonCurrentLiability,BorrowingFromCentralBank,DepositOfInterbank,BorrowingCapital,DerivativeLiability,SoldBuybackSecuProceeds,Deposit,ProxySecuProceeds,SubIssueSecuProceeds,DepositsReceived,AdvanceInsurance,CommissionPayable,ReinsurancePayables,CompensationPayable,PolicyDividendPayable,InsurerDepositInvestment,UnearnedPremiumReserve,OutstandingClaimReserve,LifeInsuranceReserve,LTHealthInsuranceLR,IndependenceLiability,OtherLiability,LExceptionalItems,LAdjustmentItems,TotalLiability,PaidInCapital,OtherEquityinstruments,EPreferStock,EPerpetualDebt,CapitalReserveFund,SurplusReserveFund,RetainedProfit,TreasuryStock,OtherCompositeIncome,OrdinaryRiskReserveFund,TradeRiskRSRVFd,ForeignCurrencyReportConvDiff,UncertainedInvestmentLoss,OtherReserves,SpecificReserves,SEExceptionalItems,SEAdjustmentItems,SEWithoutMI,MinorityInterests,OtherItemsEffectingSE,TotalShareholderEquity,LEExceptionalItems,LEAdjustmentItems,TotalLiabilityAndEquity from %s t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from %s where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    table, table, companyCode, companyCode, start, end)
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s-%s %s getBalanceSheet got wrong' % (start, end, symbol))
                    return pd.DataFrame()
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'CashEquivalents', 'ClientDeposit', 'TradingAssets', 'BillReceivable',
                     'DividendReceivable', 'BillAccReceivable', 'InterestReceivable', 'AccountReceivable',
                     'ContractualAssets', 'OtherReceivable', 'AdvancePayment', 'Inventories', 'BearerBiologicalAssets',
                     'DeferredExpense', 'HoldAndFSAssets', 'NonCurrentAssetIn1Year', 'OtherCurrentAssets',
                     'CAExceptionalItems', 'CAAdjustmentItems', 'TotalCurrentAssets', 'DebtInvestment',
                     'OthDebtInvestment', 'HoldToMaturityInvestments', 'HoldForSaleAssets', 'OthEquityInstrument',
                     'OthNonCurFinAssets', 'InvestmentProperty', 'LongtermEquityInvest', 'LongtermReceivableAccount',
                     'FixedAssets', 'ConstructionMaterials', 'ConstruInProcess', 'FixedAssetsLiquidation',
                     'BiologicalAssets', 'OilGasAssets', 'IntangibleAssets', 'SeatCosts', 'DevelopmentExpenditure',
                     'GoodWill', 'LongDeferredExpense', 'DeferredTaxAssets', 'OtherNonCurrentAssets',
                     'NCAExceptionalItems', 'NCAAdjustmentItems', 'TotalNonCurrentAssets', 'LoanAndAccountReceivables',
                     'SettlementProvi', 'ClientProvi', 'DepositInInterbank', 'RMetal', 'LendCapital',
                     'DerivativeAssets', 'BoughtSellbackAssets', 'LoanAndAdvance', 'InsuranceReceivables',
                     'ReceivableSubrogationFee', 'ReinsuranceReceivables', 'ReceivableUnearnedR', 'ReceivableClaimsR',
                     'ReceivableLifeR', 'ReceivableLTHealthR', 'InsurerImpawnLoan', 'FixedDeposit', 'RefundableDeposit',
                     'RefundableCapitalDeposit', 'IndependenceAccountAssets', 'OtherAssets', 'AExceptionalItems',
                     'AAdjustmentItems', 'TotalAssets', 'ShortTermLoan', 'ImpawnedLoan', 'TradingLiability',
                     'NotesPayable', 'AccountsPayable', 'NotAccountsPayable', 'ContractLiability', 'STBondsPayable',
                     'AdvanceReceipts', 'SalariesPayable', 'DividendPayable', 'TaxsPayable', 'InterestPayable',
                     'OtherPayable', 'AccruedExpense', 'DeferredProceeds', 'HoldAndFSLi', 'NonCurrentLiabilityIn1Year',
                     'OtherCurrentLiability', 'CLExceptionalItems', 'CLAdjustmentItems', 'TotalCurrentLiability',
                     'LongtermLoan', 'BondsPayable', 'LPreferStock', 'LPerpetualDebt', 'LongtermAccountPayable',
                     'LongSalariesPay', 'SpecificAccountPayable', 'EstimateLiability', 'DeferredTaxLiability',
                     'LongDeferIncome', 'OtherNonCurrentLiability', 'NCLExceptionalItems', 'NCLAdjustmentItems',
                     'TotalNonCurrentLiability', 'BorrowingFromCentralBank', 'DepositOfInterbank', 'BorrowingCapital',
                     'DerivativeLiability', 'SoldBuybackSecuProceeds', 'Deposit', 'ProxySecuProceeds',
                     'SubIssueSecuProceeds', 'DepositsReceived', 'AdvanceInsurance', 'CommissionPayable',
                     'ReinsurancePayables', 'CompensationPayable', 'PolicyDividendPayable', 'InsurerDepositInvestment',
                     'UnearnedPremiumReserve', 'OutstandingClaimReserve', 'LifeInsuranceReserve', 'LTHealthInsuranceLR',
                     'IndependenceLiability', 'OtherLiability', 'LExceptionalItems', 'LAdjustmentItems',
                     'TotalLiability', 'PaidInCapital', 'OtherEquityinstruments', 'EPreferStock', 'EPerpetualDebt',
                     'CapitalReserveFund', 'SurplusReserveFund', 'RetainedProfit', 'TreasuryStock',
                     'OtherCompositeIncome', 'OrdinaryRiskReserveFund', 'TradeRiskRSRVFd',
                     'ForeignCurrencyReportConvDiff', 'UncertainedInvestmentLoss', 'OtherReserves', 'SpecificReserves',
                     'SEExceptionalItems', 'SEAdjustmentItems', 'SEWithoutMI', 'MinorityInterests',
                     'OtherItemsEffectingSE', 'TotalShareholderEquity', 'LEExceptionalItems', 'LEAdjustmentItems',
                     'TotalLiabilityAndEquity']]
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.index = df['EndDate']
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.BALANCE_SHEET_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')
        return df

    ## 未加入科创板
    def getIncomeSheet(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select t1.InfoPublDate, t1.EndDate, TotalOperatingRevenue,OperatingRevenue,NetInterestIncome,InterestIncome,InterestExpense,NetCommissionIncome,CommissionIncome,CommissionExpense,NetProxySecuIncome,NetSubIssueSecuIncome,NetTrustIncome,PremiumsEarned,PremiumsIncome,ReinsuranceIncome,Reinsurance,UnearnedPremiumReserve,OtherOperatingRevenue,SpecialItemsOR,AdjustmentItemsOR,TotalOperatingCost,OperatingPayout,RefundedPremiums,CompensationExpense,AmortizationExpense,PremiumReserve,AmortizationPremiumReserve,PolicyDividendPayout,ReinsuranceCost,OperatingAndAdminExpense,AmortizationReinsuranceCost,InsuranceCommissionExpense,OtherOperatingCost,OperatingCost,OperatingTaxSurcharges,OperatingExpense,AdministrationExpense,FinancialExpense,InterestFinExp,InterestIncomeFin,RAndD,CreditImpairmentL,AssetImpairmentLoss,SpecialItemsTOC,AdjustmentItemsTOC,OtherNetRevenue,FairValueChangeIncome,InvestIncome,InvestIncomeAssociates,NetOpenHedgeIncome,ExchangeIncome,AssetDealIncome,OtherRevenue,OtherItemsEffectingOP,AdjustedItemsEffectingOP,OperatingProfit,NonoperatingIncome,NonoperatingExpense,NonCurrentAssetssDealLoss,OtherItemsEffectingTP,AdjustedItemsEffectingTP,TotalProfit,IncomeTaxCost,UncertainedInvestmentLosses,OtherItemsEffectingNP,AdjustedItemsEffectingNP,NetProfit,OperSustNetP,DisconOperNetP,NPParentCompanyOwners,MinorityProfit,OtherItemsEffectingNPP,AdjustedItemsEffectingNPP,OtherCompositeIncome,OCIParentCompanyOwners,OCIMinorityOwners,OCINotInIncomeStatement,OCIInIncomeStatement,AdjustedItemsEffectingCI,TotalCompositeIncome,CIParentCompanyOwners,CIMinorityOwners,AdjustedItemsEffectingPCI,BasicEPS,DilutedEPS from LC_IncomeStatementAll t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from LC_IncomeStatementAll where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    companyCode, companyCode, start, end)
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s-%s %s getIncomeSheet got wrong' % (start, end, symbol))
                    return pd.DataFrame()
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'TotalOperatingRevenue', 'OperatingRevenue', 'NetInterestIncome',
                     'InterestIncome', 'InterestExpense', 'NetCommissionIncome', 'CommissionIncome',
                     'CommissionExpense', 'NetProxySecuIncome', 'NetSubIssueSecuIncome', 'NetTrustIncome',
                     'PremiumsEarned', 'PremiumsIncome', 'ReinsuranceIncome', 'Reinsurance', 'UnearnedPremiumReserve',
                     'OtherOperatingRevenue', 'SpecialItemsOR', 'AdjustmentItemsOR', 'TotalOperatingCost',
                     'OperatingPayout', 'RefundedPremiums', 'CompensationExpense', 'AmortizationExpense',
                     'PremiumReserve', 'AmortizationPremiumReserve', 'PolicyDividendPayout', 'ReinsuranceCost',
                     'OperatingAndAdminExpense', 'AmortizationReinsuranceCost', 'InsuranceCommissionExpense',
                     'OtherOperatingCost', 'OperatingCost', 'OperatingTaxSurcharges', 'OperatingExpense',
                     'AdministrationExpense', 'FinancialExpense', 'InterestFinExp', 'InterestIncomeFin', 'RAndD',
                     'CreditImpairmentL', 'AssetImpairmentLoss', 'SpecialItemsTOC', 'AdjustmentItemsTOC',
                     'OtherNetRevenue', 'FairValueChangeIncome', 'InvestIncome', 'InvestIncomeAssociates',
                     'NetOpenHedgeIncome', 'ExchangeIncome', 'AssetDealIncome', 'OtherRevenue', 'OtherItemsEffectingOP',
                     'AdjustedItemsEffectingOP', 'OperatingProfit', 'NonoperatingIncome', 'NonoperatingExpense',
                     'NonCurrentAssetssDealLoss', 'OtherItemsEffectingTP', 'AdjustedItemsEffectingTP', 'TotalProfit',
                     'IncomeTaxCost', 'UncertainedInvestmentLosses', 'OtherItemsEffectingNP',
                     'AdjustedItemsEffectingNP', 'NetProfit', 'OperSustNetP', 'DisconOperNetP', 'NPParentCompanyOwners',
                     'MinorityProfit', 'OtherItemsEffectingNPP', 'AdjustedItemsEffectingNPP', 'OtherCompositeIncome',
                     'OCIParentCompanyOwners', 'OCIMinorityOwners', 'OCINotInIncomeStatement', 'OCIInIncomeStatement',
                     'AdjustedItemsEffectingCI', 'TotalCompositeIncome', 'CIParentCompanyOwners', 'CIMinorityOwners',
                     'AdjustedItemsEffectingPCI', 'BasicEPS', 'DilutedEPS']]
            df.index = pd.to_datetime(df['EndDate'])
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.INCOME_NAME_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')

        return df

    ## 未加入科创板
    def getCashFlow(self, start, end, symbol):
        """
        获取现金流量表
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'Select t1.InfoPublDate as InfoPublDate, t2.EndDate as EndDate, GoodsSaleServiceRenderCash,TaxLevyRefund,NetDepositIncrease,NetBorrowingFromCentralBank,NetBorrowingFromFinanceCo,InterestAndCommissionCashIn,NetDealTradingAssets,NetBuyBack,NetOriginalInsuranceCash,NetReinsuranceCash,NetInsurerDepositInvestment,OtherCashInRelatedOperate,SpecialItemsOCIF,AdjustmentItemsOCIF,SubtotalOperateCashInflow,GoodsServicesCashPaid,StaffBehalfPaid,AllTaxesPaid,NetLoanAndAdvanceIncrease,NetDepositInCBAndIB,NetLendCapital,CommissionCashPaid,OriginalCompensationPaid,NetCashForReinsurance,PolicyDividendCashPaid,OtherOperateCashPaid,SpecialItemsOCOF,AdjustmentItemsOCOF,SubtotalOperateCashOutflow,AdjustmentItemsNOCF,NetOperateCashFlow,InvestWithdrawalCash,Investproceeds,FixIntanOtherAssetDispoCash,NetCashDealSubCompany,OtherCashFromInvestAct,SpecialItemsICIF,AdjustmentItemsICIF,SubtotalInvestCashInflow,FixIntanOtherAssetAcquiCash,InvestCashPaid,NetCashFromSubCompany,ImpawnedLoanNetIncrease,OtherCashToInvestAct,SpecialItemsICOF,AdjustmentItemsICOF,SubtotalInvestCashOutflow,AdjustmentItemsNICF,NetInvestCashFlow,CashFromInvest,CashFromMinoSInvestSub,CashFromBondsIssue,CashFromBorrowing,OtherFinanceActCash,SpecialItemsFCIF,AdjustmentItemsFCIF,SubtotalFinanceCashInflow,BorrowingRepayment,DividendInterestPayment,ProceedsFromSubToMinoS,OtherFinanceActPayment,SpecialItemsFCOF,AdjustmentItemsFCOF,SubtotalFinanceCashOutflow,AdjustmentItemsNFCF,NetFinanceCashFlow,ExchanRateChangeEffect,OtherItemsEffectingCE,AdjustmentItemsCE,CashEquivalentIncrease,BeginPeriodCash,OtherItemsEffectingCEI,AdjustmentItemsCEI,EndPeriodCashEquivalent from LC_CashFlowStatementAll t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from LC_CashFlowStatementAll where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    companyCode, companyCode, start, end)
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s-%s %s getCashFlow got wrong' % (start, end, symbol))
                    return pd.DataFrame()
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'GoodsSaleServiceRenderCash', 'TaxLevyRefund', 'NetDepositIncrease',
                     'NetBorrowingFromCentralBank', 'NetBorrowingFromFinanceCo', 'InterestAndCommissionCashIn',
                     'NetDealTradingAssets', 'NetBuyBack', 'NetOriginalInsuranceCash', 'NetReinsuranceCash',
                     'NetInsurerDepositInvestment', 'OtherCashInRelatedOperate', 'SpecialItemsOCIF',
                     'AdjustmentItemsOCIF', 'SubtotalOperateCashInflow', 'GoodsServicesCashPaid', 'StaffBehalfPaid',
                     'AllTaxesPaid', 'NetLoanAndAdvanceIncrease', 'NetDepositInCBAndIB', 'NetLendCapital',
                     'CommissionCashPaid', 'OriginalCompensationPaid', 'NetCashForReinsurance',
                     'PolicyDividendCashPaid', 'OtherOperateCashPaid', 'SpecialItemsOCOF', 'AdjustmentItemsOCOF',
                     'SubtotalOperateCashOutflow', 'AdjustmentItemsNOCF', 'NetOperateCashFlow', 'InvestWithdrawalCash',
                     'Investproceeds', 'FixIntanOtherAssetDispoCash', 'NetCashDealSubCompany', 'OtherCashFromInvestAct',
                     'SpecialItemsICIF', 'AdjustmentItemsICIF', 'SubtotalInvestCashInflow',
                     'FixIntanOtherAssetAcquiCash', 'InvestCashPaid', 'NetCashFromSubCompany',
                     'ImpawnedLoanNetIncrease', 'OtherCashToInvestAct', 'SpecialItemsICOF', 'AdjustmentItemsICOF',
                     'SubtotalInvestCashOutflow', 'AdjustmentItemsNICF', 'NetInvestCashFlow', 'CashFromInvest',
                     'CashFromMinoSInvestSub', 'CashFromBondsIssue', 'CashFromBorrowing', 'OtherFinanceActCash',
                     'SpecialItemsFCIF', 'AdjustmentItemsFCIF', 'SubtotalFinanceCashInflow', 'BorrowingRepayment',
                     'DividendInterestPayment', 'ProceedsFromSubToMinoS', 'OtherFinanceActPayment', 'SpecialItemsFCOF',
                     'AdjustmentItemsFCOF', 'SubtotalFinanceCashOutflow', 'AdjustmentItemsNFCF', 'NetFinanceCashFlow',
                     'ExchanRateChangeEffect', 'OtherItemsEffectingCE', 'AdjustmentItemsCE', 'CashEquivalentIncrease',
                     'BeginPeriodCash', 'OtherItemsEffectingCEI', 'AdjustmentItemsCEI', 'EndPeriodCashEquivalent']]
            df.index = pd.to_datetime(df['EndDate'])
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.CASH_FLOW_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')

        return df

    def get_stockCompanyCode(self, symbol):
        """
        根据交易所代码返回company_code序列
        :param symbol: '000001.sz'
        :return: innerCode: 3
        """
        result = None
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT InnerCode, CompanyCode FROM vwu_SecuMain WHERE SecuCode = \'%s\' AND SecuMarket in (83, 90) AND SecuCategory=1' % symbol[
                                                                                                                                                 : 6]

                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'secuCode'       000001
                # 'InnerCode'    'sz'/'sh'
                for row in cursor:
                    result = row['CompanyCode']
        return result

    def get_SymbolFromCompanyCode(self, companyCode):
        """
        InnerCode 转 Symbol
        :param symbol: '000001.sz'
        :return: innerCode: 3
        """
        result = {}
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT InnerCode, SecuCode,SecuMarket FROM vwu_SecuMain WHERE CompanyCode = \'%s\' AND SecuMarket in (83, 90) AND SecuCategory=1' % companyCode
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'secuCode'       000001
                # 'InnerCode'    'sz'/'sh'
                for row in cursor:
                    result = {'Symbol': row['SecuCode'],
                              'MdSymbol': row['SecuCode'] + '.sh' if row['SecuMarket'] == 83 else row[
                                                                                                      'SecuCode'] + '.sz'}
        return result

    def getCapitalStock(self, start, end, symbol):
        """
        公司股本结构变动
        :param start: 20190101
        :param end: 20200420
        :param symbol: 600000
        :return:
        """
        companyCode = self.get_stockCompanyCode(symbol)
        tradingDay = pd.DataFrame({'EndDate': self.get_tradingday('19900101', end)})

        totalshares = []
        ashares = []
        afloats = []
        restrictedAShares = []
        endDate = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                table = 'LC_ShareStru'
                if symbol.startswith('688'):
                    table = 'LC_STIBShareStru'
                stmt = 'select EndDate, TotalShares, Ashares, AFloats, RestrictedAShares from %s where CompanyCode = %d and EndDate <= \'%s\' ' % (
                    table, companyCode, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'Symbol'       '600000.sh'
                # 'ExdiviDate'    '20150101'
                # 'AdjustingFactor'    1.0
                # 'AdjustConst'    1.0
                # 'RatioAdjustingFactor'    1.0
                for row in cursor:
                    endDate.append(row['EndDate'].strftime('%Y%m%d'))
                    totalshares.append(float(row['TotalShares']) if row['TotalShares'] else NaN)
                    ashares.append(float(row['Ashares']) if row['Ashares'] else NaN)
                    restrictedAShares.append(float(row['RestrictedAShares']) if row['RestrictedAShares'] else NaN)
                    afloats.append(float(row['AFloats']) if row['AFloats'] else NaN)

        df = pd.DataFrame(
            {'EndDate': endDate, '总股本': totalshares, '流通A股': afloats, '限制性流通A股': restrictedAShares,
             'A股股本': ashares})

        data = pd.merge(tradingDay, df, how='left', on=['EndDate'])
        data.index = pd.to_datetime(data['EndDate'])
        data.index.name = ''
        # data = data.reindex(pd.to_datetime(data['EndDate']))
        data.fillna(method='ffill', inplace=True)
        data.fillna(method='bfill', inplace=True)
        data[(data.index >= start) & (data.index <= end)]
        data = data[['总股本', 'A股股本', '流通A股', '限制性流通A股']]
        data['自由流通股'] = NaN
        return data

    def getCapitalStockByDay(self, tradingDay):
        """
        公司股本结构变动--按天
        :param tradingDay: 20200401
        :return:
        """
        totalshares = []
        ashares = []
        afloats = []
        restrictedAShares = []
        Companys = []
        Symbols = []
        MdSymbols = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select CompanyCode,TotalShares, Ashares, AFloats, RestrictedAShares from LC_ShareStru where EndDate = \'%s\'' % (
                    tradingDay)
                cursor.execute(stmt)
                for row in cursor:
                    Companys.append(int(row['CompanyCode']) if row['CompanyCode'] else NaN)
                    dic = self.get_SymbolFromCompanyCode(int(row['CompanyCode']))
                    Symbols.append(str(dic['Symbol'].zfill(6)))
                    MdSymbols.append(dic['MdSymbol'])
                    totalshares.append(float(row['TotalShares']) if row['TotalShares'] else NaN)
                    ashares.append(float(row['Ashares']) if row['Ashares'] else NaN)
                    restrictedAShares.append(float(row['RestrictedAShares']) if row['RestrictedAShares'] else NaN)
                    afloats.append(float(row['AFloats']) if row['AFloats'] else NaN)

                stmt = 'select CompanyCode,TotalShares, Ashares, AFloats, RestrictedAShares from LC_STIBShareStru where EndDate = \'%s\'' % (
                    tradingDay)
                cursor.execute(stmt)
                for row in cursor:
                    print(row['CompanyCode'])
                    Companys.append(int(row['CompanyCode']) if row['CompanyCode'] else NaN)
                    dic = self.get_SymbolFromCompanyCode(int(row['CompanyCode']))
                    Symbols.append(str(dic['Symbol'].zfill(6)))
                    MdSymbols.append(dic['MdSymbol'])
                    totalshares.append(float(row['TotalShares']) if row['TotalShares'] else NaN)
                    ashares.append(float(row['Ashares']) if row['Ashares'] else NaN)
                    restrictedAShares.append(float(row['RestrictedAShares']) if row['RestrictedAShares'] else NaN)
                    afloats.append(float(row['AFloats']) if row['AFloats'] else NaN)

        df = pd.DataFrame({'证券代码': Symbols, '证券代码(市场)': MdSymbols,
                           '总股本': totalshares, '流通A股': afloats, '限制性流通A股': restrictedAShares,
                           'A股股本': ashares})
        return df

    # 不包括科创板
    def getFinancialDerivative(self, start, end, symbol):
        """
        公司额外财务分析指标 销售毛利率 营业利润率 销售费用率 净利润率 EBIT利润率
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)

        df = pd.DataFrame()
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'IF OBJECT_ID(\'tempdb..#tmpData\') IS NOT NULL\n  DROP TABLE #tmpData'
                cursor.execute(stmt)
                stmt = 'IF OBJECT_ID(\'tempdb..#tmpDate\') IS NOT NULL\n DROP TABLE #tmpDate'
                cursor.execute(stmt)

                stmt = 'select EndDate, ROE, ROA, ROIC, GrossIncomeRatio as \'销售毛利率\' ,OperatingProfitRatio as \'营业利润率\',OperatingExpenseRate as \'销售费用率\', NPToTOR as \'净利润率\' ,EBITToTOR as \'EBIT利润率\', EBITDA/ (EBIT / nullif(EBITToTOR, 0)) AS \'EBITDA利润率\',  NetProfitCut / (EBIT / nullif(EBITToTOR, 0)) as \'扣非净利率\' into #tmpData from LC_MainIndexNew where CompanyCode = %s and EndDate >= \'%s\' and EndDate <= \'%s\' ' % (
                    companyCode, start, end)
                cursor.execute(stmt)

                stmt = 'select InfoPublDate, EndDate into #tmpDate from LC_MainDataNew where CompanyCode = %s and Mark = 2' % companyCode
                cursor.execute(stmt)

                stmt = 'select t1.*, t2.InfoPublDate as \'发布日\' from #tmpData t1 left JOIN #tmpDate t2 on t1.EndDate=t2.EndDate'
                cursor.execute(stmt)
                for row in cursor:
                    df = df.append(row, ignore_index=True)

        if not df.empty:
            df.index = df['EndDate']
            df['报告日'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else None)
            df['发布日'] = df['发布日'].map(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else None)
            del df['EndDate']
            df.index.rename('', inplace=True)
        df = df.astype('float64')
        return df

    def getDailyPerformance(self, tradingDay, type):
        """
        获得日级别数据
        :param tradingDay: '20150101'
        :param type: 'MarketCap'
        :return: pd.DataFrame
        """
        if type == 'MarketCap':
            idx = 'TotalMV'
            star_idx = 'TotalMV'
        if type == 'FloatMarketCap':
            idx = 'NegotiableMV'
            star_idx = 'NegotiableMV'
        if type == 'Y1Volatility':
            idx = 'Y1Volatility'
            star_idx = 'YVolatility'
        if type == 'Ret':
            idx = 'ChangePCT'
            star_idx = 'ChangePCT'
        if type == 'TurnoverRate':
            idx = 'TurnoverRate'
            star_idx = 'TurnoverRate'
        if type == 'TurnoverRateRW':
            idx = 'TurnoverRateRW'
            star_idx = 'TurnoverRateRW'
        if type == 'TurnoverRatePerDayRW':
            idx = 'TurnoverRatePerDayRW'
            star_idx = 'TurnoverRatePerDayRW'
        if type == 'TurnoverRateRM':
            idx = 'TurnoverRateRM'
            star_idx = 'TurnoverRateRM'
        if type == 'TurnoverRatePerDayRM':
            idx = 'TurnoverRatePerDayRM'
            star_idx = 'TurnoverRatePerDayRM'
        if type == 'TurnoverRateR3M':
            idx = 'TurnoverRateR3M'
            star_idx = 'TurnoverRateRMThree'
        if type == 'TurnoverRateR6M':
            idx = 'TurnoverRateR6M'
            star_idx = 'TurnoverRateRMSix'
        if type == 'TurnoverRateR12M':
            idx = 'TurnoverRateR12M'
            star_idx = 'TurnoverRateRY'

        symbol = []
        value = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                if type == 'MarketCap' or type == 'FloatMarketCap':
                    stmt = 'select t1.%s / 10000 as type , case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as symbol from LC_STIBPerformance t1 LEFT JOIN vwu_SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.TradingDay = \'%s\' and t2.SecuMarket in (83, 90) and t2.SecuCategory = 1 union all select t1.%s as type, case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as symbol from QT_Performance t1 LEFT JOIN vwu_SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.TradingDay = \'%s\' and t2.SecuMarket in (83, 90) and t2.SecuCategory = 1' % (
                        star_idx, tradingDay, idx, tradingDay)
                else:
                    stmt = 'select t1.%s as type , case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as symbol from LC_STIBPerformance t1 LEFT JOIN vwu_SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.TradingDay = \'%s\' and t2.SecuMarket in (83, 90) and t2.SecuCategory = 1 union all select t1.%s as type, case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as symbol from QT_Performance t1 LEFT JOIN vwu_SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.TradingDay = \'%s\' and t2.SecuMarket in (83, 90) and t2.SecuCategory = 1' % (
                        star_idx, tradingDay, idx, tradingDay)
                cursor.execute(stmt)

                for row in cursor:
                    symbol.append(row['symbol'])
                    value.append(row['type'])

        df = pd.DataFrame(data={'symbol': symbol, type: value})
        return df

    def getLimitStatus(self, tradingDay, type):
        """
        返回是否涨跌停
        :param tradingDay: '20150101'
        :param type: 'SurgedLimit' 'DeclineLimit'
        :return: pd.DataFrame
        """
        symbol = []
        value = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select t1.%s, case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as symbol from QT_PerformanceData t1 LEFT JOIN vwu_SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.TradingDay = \'%s\' and t2.SecuMarket in (83, 90) and t2.SecuCategory = 1' % (
                    type, tradingDay)
                cursor.execute(stmt)

                for row in cursor:
                    symbol.append(row['symbol'])
                    value.append(row[type])

        df = pd.DataFrame(data={'symbol': symbol, type: value})
        return df

    def getDailyDerivative(self, start, end, symbol):
        """
        获取公司估值分析日指标
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        # companyCode = self.get_stockCompanyCode(symbol)

        innerCode = self.get_stockInnerCode(symbol)
        if innerCode is None:
            return pd.DataFrame()
        tradingDay = pd.to_datetime(self.get_tradingday(start, end))

        # df = pd.DataFrame()
        endDate = []
        turnover = []
        mv = []
        nego_mv = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                table = 'QT_Performance'
                if symbol.startswith('688'):
                    table = 'LC_STIBPerformance'
                stmt = 'select TradingDay as EndDate, TurnoverRate, TotalMV, NegotiableMV from %s where InnerCode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\'' % (
                    table, innerCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    endDate.append(row['EndDate'])
                    turnover.append(row['TurnoverRate'])
                    mv.append(row['TotalMV'])
                    nego_mv.append(row['NegotiableMV'])
                    # df = df.append(row, ignore_index=True)
        df = pd.DataFrame(index=endDate, data={'换手率': turnover, '总市值': mv, '流通市值': nego_mv})
        if not df.empty:
            df = df.reindex(tradingDay)
            df.index.set_names('', inplace=True)

        tradingDay = []
        pe = []
        pettm = []
        pb = []
        ps = []
        psttm = []
        dividend = []
        ew = []
        ewn = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                table = 'LC_DIndicesForValuation'
                PE_STR = 'PE'
                DIVIDEND_RATIO_LYR_STR = 'DividendRatioLYR'
                if symbol.startswith('688'):
                    table = 'LC_STIBDIndiForValue'
                    PE_STR = 'PETTM'
                    DIVIDEND_RATIO_LYR_STR = 'DividendRatio'
                stmt = 'select TradingDay, PELYR as PE, %s as PETTM, PB, PS, PSTTM, %s AS DividendRatioLYR,  EnterpriseValueW, EnterpriseValueN from %s where InnerCode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\'' % (
                    PE_STR, DIVIDEND_RATIO_LYR_STR, table, innerCode, start, end)
                cursor.execute(stmt)

                for row in cursor:
                    tradingDay.append(row['TradingDay'])
                    pe.append(row['PE'])
                    pettm.append(row['PETTM'])
                    pb.append(row['PB'])
                    ps.append(row['PS'])
                    psttm.append(row['PSTTM'])
                    dividend.append(row['DividendRatioLYR'])
                    ew.append(row['EnterpriseValueW'])
                    ewn.append(row['EnterpriseValueN'])
        df1 = pd.DataFrame(index=tradingDay,
                           data={'PE': pe, 'PETTM': pettm, 'PB': pb, 'PS': ps, 'PSTTM': psttm, '股息率': dividend,
                                 '企业价值': ew, '企业价值不含货币': ewn})
        if not df1.empty:
            df1 = df1.reindex(tradingDay)
            df1.index.set_names('', inplace=True)
        df = pd.concat([df, df1], axis=1)
        df.fillna(method='ffill', inplace=True)
        df = df.astype('float64')

        return df


if __name__ == '__main__':
    source = JydbSource()

    # df = source.get_universe('20190722')
    # print(df)
    # df = source.getIndexFutureDayBarByDay('20190301')
    # print(df)
    # df = source.getIndexFutureDayBar('IF1903', '20190301', ' 20190320')
    # print(df)
    # df = source.getIndexFutureTradable('20190327')
    # print(df)

    # df = source.get_month_end_tradingday('20180101', '20181231')
    # print(df)

    # df = source.getLimitStatus('20190722', 'SurgedLimit')
    # print(df.head())

    # test getDailyPerformance
    # df = source.getDailyPerformance('20190722', 'TurnoverRateR12M')
    # print(df.head())

    # test getDailyDerivative
    df = source.getDailyDerivative('20190722', '20190722', '600000.SH')
    print(df.head())

    # test getFinancialDerivative
    # df = source.getFinancialDerivative('20180101', '20190722', '600000.sh')
    # print(df.head())

    # test getCapitalStock
    # stock = source.getCapitalStock('20190701', '20190722', '600000.sh')
    # print(stock.head())

    # test getBalaceSheet
    # balance_sheet = source.getBalanceSheet('20170101', '20190722', '600000.sh')
    # print(balance_sheet.head())

    # test getIncomeSheet
    # income_sheet = source.getIncomeSheet('20000101', '20020101', '600000.sh')
    # print(income_sheet.head())

    # test getCashFlow
    # cashFlow_sheet = source.getCashFlow('20180101', '20200101', '600000.sh')
    # print(cashFlow_sheet.head())

    # test getCompanyCode
    # companyCode = source.get_stockCompanyCode('600000.sh')
    # print(companyCode)

    # test getInnerCode
    # innerCode = source.get_stockInnerCode('688001.sh')
    # print(innerCode)
    # test getDividend
    # dividend = source.getDividend('20181101', '20181107')
    # print(dividend.head())

    # test getIndexComponentWeight
    # index_component_weight = source.getIndexComponentWeight('000300', '20181228')
    # print(index_component_weight.head())

    # test getIndexWeight
    # index_weight = source.getIndexWeight('801010', '20100101')
    # print(index_weight.head())

    # test getIndexDayBar
    # indexDayBar = source.getIndexDayBar('000905', '20180901', '20180914')
    # print(indexDayBar.head())

    # test getIndexUniverse
    # index_universe = source.getIndexUniverse('20130621')
    # print(index_universe.head())
    #
    # test getDayBarByDay
    # td = source.getDayBarByDay('20190722')
    # print(td.head())
    # td = source.getDayBar('600000.sh', '20190722', '20190722')
    # print(td)

    # df = source.getLimitStatus('20190729', 'SurgedLimit')
    # print(df)
