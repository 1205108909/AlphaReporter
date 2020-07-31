#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : JYDataLoader.py
@Time : 2020/4/26 12:10
"""

import pymssql
import numpy as np
import pandas as pd
import time
from datetime import datetime
import multiprocessing
import threading

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class JYDataLoader:
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls, *args, **kwargs):
        with JYDataLoader._instance_lock:
            if not hasattr(JYDataLoader, "_instance"):
                JYDataLoader._instance = JYDataLoader(*args, **kwargs)
        return JYDataLoader._instance

    def __init__(self):
        self.server = "172.10.10.9"
        self.user = "jydb1"
        self.password = "jydb1"
        self.database = "jydb1"
        self.conn = None
        self.earliestDay = '19900101'

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                print(e)

    def getTradableList(self, tradingDay):
        mdSymbols = []
        innerCodes = []
        secuMarkets = []
        sTypes = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableSecurity'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] == 'BDC' or row['sType'] == 'FTR' or row['isTradable'] == 0:
                        continue
                    mdSymbols.append(row['stockId'])
                    innerCodes.append(row['innerCode'])
                    secuMarkets.append(row['secuMarket'])
                    sTypes.append(row['sType'])

        df = pd.DataFrame(
            {'symbol': mdSymbols, 'innerCode': innerCodes,
             'secuMarkets': secuMarkets, 'sType': sTypes})
        df = df[(df['secuMarkets'].isin([83, 90])) & (df['sType'] == 'EQA') & (~df['symbol'].str.startswith('688'))]
        return df

    def getAStock(self, isAlive=1):
        mdSymbols = []
        innerCodes = []
        secuMarkets = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetAStock'
                cursor.callproc(proc, (isAlive,))
                for row in cursor:
                    mdSymbols.append(row['stockSymbol'])
                    innerCodes.append(row['InnerCode'])
                    secuMarkets.append(row['secuMarket'])

        df = pd.DataFrame(
            {'symbol': mdSymbols, 'innerCode': innerCodes,
             'secuMarkets': secuMarkets})
        return df

    def get_tradingday(self, start, end):
        """
        返回交易日序列
        :param start: '20150101'
        :param end: '20150130'
        :return: tradingDay: ['20150101', '20150101']
        """
        try:
            tradingDay = []
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    proc = 'spu_GetTradingDate'
                    cursor.callproc(proc, (start, end))
                    for row in cursor:
                        tradingDay.append(row['TradingDate'])
            return tradingDay
        except Exception as e:
            raise Exception("%s get_tradingday Fail")
            print(e)
            return pd.DataFrame()

    def getLastDay(self, date, start, end):
        tradingDays = self.get_tradingday(start, end)
        tradingDays = sorted(tradingDays)
        try:
            index = tradingDays.index(date)
            if index > 0:
                return tradingDays[index - 1]
            elif index == 0:
                return tradingDays[0]
            elif index < 0:
                raise Exception("tradingDayDB is not in tradingDays")

        except Exception as e:
            self.logger.error(e)
            return None

    def getInnerCodeFromSymbol(self, symbol, isAlive=False):
        stmt = 'select * from vwu_SecuMain where SecuCategory = 1 and SecuMarket In (83,90) and SecuCode = \'%s\'' % symbol[
                                                                                                                     0:6]
        if isAlive:
            stmt += ' and ListedState = 1'
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(stmt)
                for row in cursor:
                    if len(cursor.fetchall()) > 1:
                        raise Exception("getInnerCodeFromSymbol %s 查出多条InnerCode" % symbol)
                    innerCode = row['InnerCode']
        return innerCode

    def get_inner_code(self, symbols, isAlive=1):
        """
        通过symbols得到对应的InnerCodes
        :param symbols:List<InnerCode>
        :param isAlive:是否是已存在的symbol
        :return:df
        """
        if len(symbols) > len(set(symbols)):
            raise Exception("Error:get_inner_code输入参数：symbols中有重复")

        mdSymbols = []
        innerCodes = []
        secuMarkets = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetAStock'
                cursor.callproc(proc, (isAlive,))
                for row in cursor:
                    mdSymbols.append(row['stockSymbol'])
                    innerCodes.append(row['InnerCode'])
                    secuMarkets.append(row['secuMarket'])

        df = pd.DataFrame(
            {'mdSymbol': mdSymbols, 'innerCode': innerCodes,
             'secuMarket': secuMarkets})

        df = df[df['mdSymbol'].isin(symbols)]
        if len(symbols) < len(df):
            raise Exception("Error:get_inner_code中一个SecuCode对应多个InnerCode")
        if len(symbols) > len(df):
            raise Exception("Error:get_inner_code中一个InnerCode对应多个SecuCode")

        # DataFrame 的 index 按照给定list排序
        df['mdSymbol'] = df['mdSymbol'].astype('category')
        df['mdSymbol'].cat.reorder_categories(symbols, inplace=True)
        df = df.sort_values('mdSymbol', ascending=True)
        df = pd.DataFrame(df['innerCode'].tolist(), index=df['mdSymbol'], columns=['innerCode'])
        return df

    def get_index_innercode(self, symbols, isAlive=1):
        """
        通过symbols得到对应的InnerCodes,其中SecuCode以‘80’开头,SecuMarket = 83的symbol后缀标以.si
        :param symbols:List<InnerCode>
        :param isAlive:是否是已存在的symbol
        :return:df
        """
        if len(symbols) > len(set(symbols)):
            raise Exception("Error:get_index_innercode输入参数：symbols中有重复")
        mdSymbols = []
        innerCodes = []
        secuMarkets = []

        stmt = ''
        if isAlive == 1:
            stmt += 'ListedState = 1 and '
        stmt += 'SecuCode in ('
        for symbol in symbols:  # 批量Insert临时表
            stmt += '\'%s\',' % symbol[0:6]
        stmt = stmt[0:len(stmt) - 1]
        stmt += ')'
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute('select * from vwu_SecuMain where SecuCategory = 4 and %s' % stmt)
                for row in cursor:
                    suffix = ''
                    if row['SecuMarket'] == 83:
                        if row['SecuCode'].startswith('80'):
                            suffix = '.si'
                        else:
                            suffix = '.sh'
                    elif row['SecuMarket'] == 90:
                        suffix = '.sz'
                    mdSymbols.append(row['SecuCode'] + suffix)
                    innerCodes.append(row['InnerCode'])
                    secuMarkets.append(row['SecuMarket'])

        df = pd.DataFrame(
            {'mdSymbol': mdSymbols, 'innerCode': innerCodes,
             'secuMarket': secuMarkets})

        if len(mdSymbols) < len(innerCodes):
            raise Exception("Error:get_index_innercode中一个Index对应多个InnerCode")
        if len(mdSymbols) > len(innerCodes):
            raise Exception("Error:get_index_innercode中一个InnerCode对应多个Index")

        df['mdSymbol'] = df['mdSymbol'].astype('category')
        df['mdSymbol'].cat.reorder_categories(symbols, inplace=True)
        df = df.sort_values('mdSymbol', ascending=True)
        df = pd.DataFrame(df['innerCode'].tolist(), index=df['mdSymbol'], columns=['innerCode'])
        return df

    def read_share(self, tradingdays, innerCodes, code, shift=0, innerCodestep=1000):
        """
         通过spu_GetShare返回从LC_ShareStru查找的给定标的给定交易日的code
         :param innerCodes: '[3,1120]'
         :param tradingdays: '[20200102,20200103]'
         :param code: 'NAS', 'FAS', 'RAS', 'AS', 'TS'
         :param shift: 推迟一天，数据下移一行
         :param innerCodestep: 自定义一次批量插入InnerCode的条目数
         :return:df
        """
        try:
            codes = ['NAS', 'FAS', 'RAS', 'AS', 'TS']
            innerCodeInSql = []
            effectDate = []
            shareData = []

            if not code in codes:
                raise Exception("read_share 方法 code 只能输入['NAS', 'FAS', 'RAS', 'AS', 'TS']")

            innerCodestep = innerCodestep if innerCodestep <= 1000 else 1000

            innerCodes = innerCodes['innerCode']

            allInnerCodelist = [innerCodes[i:i + innerCodestep] for i in
                                range(0, len(innerCodes), innerCodestep)]  # 针对数据量大，分段计算
            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.execute('create table #tmpStock (innerCode int, seqNb int identity)')
                    proc = 'spu_GetShare'
                    for oneInnerCodelist in allInnerCodelist:
                        stmt1 = ''
                        for innerCode in oneInnerCodelist:
                            stmt1 += '(%s),' % innerCode
                        stmt1 = stmt1[0:len(stmt1) - 1]
                        cursor.execute('insert into #tmpStock values %s' % stmt1)
                    cursor.callproc(proc, (code,))
                    for row in cursor:
                        innerCodeInSql.append(row['innerCode'])
                        effectDate.append(row['effectDate'])
                        shareData.append(row['shareData'])
                    ret_list = list(set(innerCodes) ^ set(innerCodeInSql))
                    if len(ret_list) > 0:
                        for newInnerCode in ret_list:
                            innerCodeInSql.append(newInnerCode)
                            effectDate.append(min(tradingdays))
                            shareData.append(np.NAN)

            df = pd.DataFrame(
                {'innerCode': innerCodeInSql, 'effectDate': effectDate,
                 'shareData': shareData})

            df = df.pivot(index='effectDate', columns='innerCode', values='shareData')

            alldays = self.get_tradingday(self.earliestDay, max(tradingdays))
            alldays = list(set(alldays + df.index.tolist()))  # 解决公告日非交易日的情况

            dfitems = {}  # 字典：键InnerCode 值DF InnerCode与allDays组成的空DF
            for item in innerCodes:
                dfitems[item] = pd.DataFrame({'innerCode': item, 'effectDate': alldays, 'shareData': np.NAN})
            all_empty_df = pd.concat(dfitems.values())

            all_empty_df = all_empty_df.pivot(index='effectDate', columns='innerCode', values='shareData')
            df = df.reindex(all_empty_df.index).fillna(method='ffill')

            df = df[df.index.isin(tradingdays)]
            df = df[innerCodes.tolist()]
            df = df.shift(shift)
            return df
        except Exception as e:
            raise Exception("%s read_share Fail" % code)
            print(e)
            return pd.DataFrame()

    def read_price(self, tradingdays, innerCodes, code, shift=0, tradingdaystep=1000, innerCodestep=1000):
        """
         通过spu_GetShare返回从LC_ShareStru查找的给定标的给定交易日的code
         :param symbols: '[000001.sz,000002.sz]'
         :param tradingdays: '[20200102,20200103]'
         :param code: 'PC', 'PO', 'PH', 'PL', 'VM', 'AM', 'VD', 'PPC'
         :param shift: 推迟一天，数据下移一行
         :param step: 以step为值拆分tradingdays
         :param innerCodestep: 自定义一次批量插入InnerCode的条目数
         :param tradingdaystep: 自定义一次批量插入Tradingday的条目数
         :return:
        """
        try:
            codes = ['PC', 'PO', 'PH', 'PL', 'VM', 'AM', 'VD', 'PPC']
            innerCodeInSql = []
            tradingday = []
            price = []

            if not code in codes:
                raise Exception("read_price 方法 code 只能输入['PC', 'PO', 'PH', 'PL', 'VM','AM', 'VD', 'PPC']")

            tradingdaystep = tradingdaystep if tradingdaystep <= 1000 else 1000  # 判断批量插入步数的合法性，一次批量插入上限1000条
            innerCodestep = innerCodestep if innerCodestep <= 1000 else 1000
            innerCodes = innerCodes['innerCode']
            allDaylist = [tradingdays[i:i + tradingdaystep] for i in
                          range(0, len(tradingdays), tradingdaystep)]  # 数据量大，分段计算
            allInnerCodelist = [innerCodes[i:i + innerCodestep] for i in
                                range(0, len(innerCodes), innerCodestep)]  # 数据量大，分段计算

            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.execute('create table #tmpStock (innerCode int, seqNb int identity)')
                    cursor.execute('create table #tmpTradingDay(tradingDay smalldatetime)')
                    proc = 'spu_GetTechData'
                    for oneInnerCodelist in allInnerCodelist:  # 批量Insert临时表
                        stmt1 = ''
                        for innerCode in oneInnerCodelist:
                            stmt1 += '(%s),' % innerCode
                        stmt1 = stmt1[0:len(stmt1) - 1]
                        cursor.execute('insert into #tmpStock values %s' % stmt1)

                    for oneDaylist in allDaylist:
                        stmt = ''
                        for day in oneDaylist:
                            stmt += '(\'%s\'),' % day
                        stmt = stmt[0:len(stmt) - 1]
                        cursor.execute('insert into #tmpTradingDay values %s' % stmt)
                    cursor.callproc(proc, (code,))
                    # 耗时20s
                    # print('data方法开始时间:%s' % datetime.now())
                    # cursor.nextset()
                    # data = cursor.fetchall()
                    # print('data方法结束时间:%s' % datetime.now())
                    for item in cursor:
                        innerCodeInSql.append(item['innerCode'])
                        tradingday.append(item['tradingDay'])
                        price.append(item['price'])

            df = pd.DataFrame(
                {'innerCodeInSql': innerCodeInSql, 'tradingday': tradingday,
                 'price': price})
            df.sort_values(by=['innerCodeInSql', 'tradingday'], inplace=True)
            # 重新组表
            df = df.pivot(index='tradingday', columns='innerCodeInSql', values='price')
            df = df[innerCodes.tolist()]
            # 设置偏移
            df = df.shift(shift)
            return df

        except Exception as e:
            raise Exception("%s read_price Fail" % code)
            print(e)
            return pd.DataFrame()

    def read_adjustingFactor(self, tradingdays, innerCodes, code='SF', shift=0, innerCodestep=1000):
        """
         通过spu_GetAdjustingFactor返回从QT_AdjustingFactor查找的给定标的给定交易日的code
         :param symbols: '[000001.sz,000002.sz]'
         :param tradingdays: '[20200102,20200103]'
         :param code: 'SF'
         :param shift: 推迟一天，数据下移一行
         :param innerCodestep: 自定义一次批量插入InnerCode的条目数
         :return:
        """
        try:
            innerCodeInSql = []
            exDiviDate = []
            splitFactor = []
            if code != 'SF':
                raise Exception("read_adjustingFactor 方法 code 只能输入SF")

            innerCodestep = innerCodestep if innerCodestep <= 1000 else 1000
            innerCodes = innerCodes['innerCode']
            allInnerCodelist = [innerCodes[i:i + innerCodestep] for i in
                                range(0, len(innerCodes), innerCodestep)]  # 针对数据量大，分段计算

            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.execute('create table #tmpStock (innerCode int, seqNb int identity)')
                    proc = 'spu_GetAdjustingFactor'
                    for oneInnerCodelist in allInnerCodelist:  # 批量Insert临时表
                        stmt1 = ''
                        for innerCode in oneInnerCodelist:
                            stmt1 += '(%s),' % innerCode
                        stmt1 = stmt1[0:len(stmt1) - 1]
                        cursor.execute('insert into #tmpStock values %s' % stmt1)
                    cursor.callproc(proc, (code,))
                    for row in cursor:
                        innerCodeInSql.append(row['innerCode'])
                        exDiviDate.append(row['exDiviDate'])
                        splitFactor.append(row['splitFactor'])
                    ret_list = list(set(innerCodes) ^ set(innerCodeInSql))
                    if len(ret_list) > 0:
                        for newInnerCode in ret_list:
                            innerCodeInSql.append(newInnerCode)
                            exDiviDate.append(min(tradingdays))
                            splitFactor.append(np.NAN)

            df = pd.DataFrame(
                {'innerCode': innerCodeInSql, 'exDiviDate': exDiviDate,
                 'splitFactor': splitFactor})

            df = df.pivot(index='exDiviDate', columns='innerCode', values='splitFactor')

            alldays = self.get_tradingday(self.earliestDay, max(tradingdays))
            alldays = list(set(alldays + df.index.tolist()))  # 解决公告日非交易日的情况

            dfitems = {}  # 字典：键InnerCode 值DF InnerCode与allDays组成的空DF
            for item in innerCodes:
                dfitems[item] = pd.DataFrame({'innerCode': item, 'exDiviDate': alldays, 'splitFactor': np.NAN})
            all_empty_df = pd.concat(dfitems.values())

            all_empty_df = all_empty_df.pivot(index='exDiviDate', columns='innerCode', values='splitFactor')
            df = df.reindex(all_empty_df.index).fillna(method='ffill')
            df = df[df.index.isin(tradingdays)]
            df = df[innerCodes.tolist()]
            df = df.shift(shift)
            return df

        except Exception as e:
            raise Exception("%s read_adjustingFactor Fail" % code)
            print(e)
            return pd.DataFrame()

    def read_index_tech_data(self, tradingdays, innerCodes, code, shift=0, tradingdaystep=1000, innerCodestep=1000,
                             isSW=False):
        """
         通过spu_GetIndexTechData返回给定标的给定交易日的code
         :param symbols: df
         :param tradingdays: '[20200102,20200103]'
         :param code: 'PC', 'PO', 'PH', 'PL', 'VM','AM', 'VD', 'PT','MV'
         :param shift: 推迟一天，数据下移一行
         :param step: 以step为值拆分tradingdays
         :param innerCodestep: 自定义一次批量插入InnerCode的条目数
         :param tradingdaystep: 自定义一次批量插入Tradingday的条目数
         :return:
        """
        try:
            codes = ['PC', 'PO', 'PH', 'PL', 'VM', 'AM', 'VD', 'PT'] if isSW else ['PC', 'PO', 'PH', 'PL', 'VM', 'AM',
                                                                                   'VD', 'PT', 'MV']
            innerCodeInSql = []
            tradingday = []
            price = []

            if not code in codes:
                raise Exception("read_index_TechData 方法 code 只能输入['PC', 'PO', 'PH', 'PL', 'VM','AM', 'VD', 'PT','MV']")

            tradingdaystep = tradingdaystep if tradingdaystep <= 1000 else 1000  # 判断批量插入步数的合法性，一次批量插入上限1000条
            innerCodestep = innerCodestep if innerCodestep <= 1000 else 1000
            innerCodes = innerCodes['innerCode']
            allDaylist = [tradingdays[i:i + tradingdaystep] for i in
                          range(0, len(tradingdays), tradingdaystep)]  # 数据量大，分段计算
            allInnerCodelist = [innerCodes[i:i + innerCodestep] for i in
                                range(0, len(innerCodes), innerCodestep)]  # 数据量大，分段计算

            with self.get_connection() as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.execute('create table #tmpSecurity (innerCode int, seqNb int identity)')
                    cursor.execute('create table #tmpTradingDay(tradingDay smalldatetime)')

                    proc = 'spu_GetSWIndexTechData' if isSW else 'spu_GetIndexTechData'
                    for oneInnerCodelist in allInnerCodelist:  # 批量Insert临时表
                        stmt1 = ''
                        for innerCode in oneInnerCodelist:
                            stmt1 += '(%s),' % innerCode
                        stmt1 = stmt1[0:len(stmt1) - 1]
                        cursor.execute('insert into #tmpSecurity values %s' % stmt1)

                    for oneDaylist in allDaylist:
                        cursor.execute('truncate table #tmpTradingDay')  # 存入执行50条数据后清空tmpTradingDay,用于下次存入
                        stmt = ''
                        for day in oneDaylist:
                            stmt += '(\'%s\'),' % day
                        stmt = stmt[0:len(stmt) - 1]
                        cursor.execute('insert into #tmpTradingDay values %s' % stmt)
                        cursor.callproc(proc, (code,))
                        for row in cursor:
                            # stockCode.append(row['secuCode'][-6:] + '.' + row['secuCode'][0:2])
                            innerCodeInSql.append(row['innerCode'])
                            tradingday.append(row['tradingDay'])
                            price.append(row['price'])
            df = pd.DataFrame(
                {'innerCodeInSql': innerCodeInSql, 'tradingday': tradingday,
                 'price': price})

            df.sort_values(by=['innerCodeInSql', 'tradingday'], inplace=True)
            # 重新组表
            df = df.pivot(index='tradingday', columns='innerCodeInSql', values='price')
            df = df[innerCodes.tolist()]
            # 设置偏移
            df = df.shift(shift)
            return df

        except Exception as e:
            raise Exception("%s read_index_TechData Fail" % code)
            print(e)
            return pd.DataFrame()


if __name__ == '__main__':
    print(pd.__version__)
    writer = pd.ExcelWriter('./JYDataLoader.xlsx')

    univPath = 'C:/Users/hc01/Desktop/stocks.csv'
    df = pd.read_csv(univPath)
    universe = df['Symbol'].tolist()

    """ 3.0 loading daily var """

    jyloader = JYDataLoader.instance()

    allstock = jyloader.getAStock(1)
    tradableList = jyloader.getTradableList('20200709')
    allday = jyloader.get_tradingday('20100301', '20200331')
    innercode = jyloader.get_inner_code(universe)  # 此时得到symbol与InnerCode是一一对应关系，键值可以转换

    print('read_share方法开始时间:%s' % datetime.now())
    floatshare = jyloader.read_share(allday, innercode, 'NAS')
    floatshare.columns = universe  # 更换column
    print('read_share方法结束时间:%s' % datetime.now())
    floatshare.to_csv(writer, 'read_share', encoding="utf-8")

    print('read_adjustingFactor方法开始时间:%s' % datetime.now())
    ajustingFactor = jyloader.read_adjustingFactor(allday, innercode)
    ajustingFactor.columns = universe  # 更换column
    print('read_adjustingFactor方法结束时间:%s' % datetime.now())
    ajustingFactor.to_csv(writer, 'read_adjustingFactor', encoding="utf-8")

    print('read_price方法开始时间:%s' % datetime.now())
    closep = jyloader.read_price(allday, innercode, 'PC')
    closep.columns = universe  # 更换column
    print('read_price方法结束时间:%s' % datetime.now())
    closep.to_csv(writer, 'read_price', encoding="utf-8")

    universe = ['000852.sh', '000300.sh', '000905.sh', '000016.sh', '801011.si', '801001.si']
    indexInnerCode = jyloader.get_index_innercode(universe)
    inner_price_HS = jyloader.read_index_tech_data(allday, indexInnerCode, 'PC')
    inner_price_HS.columns = universe  # 更换column

    universe = ['801040.sh', '801010.sh', '801020.sh', '801030.sh']
    indexInnerCode = jyloader.get_index_innercode(universe)
    inner_price_SW = jyloader.read_index_tech_data(allday, indexInnerCode, 'PC', isSW=True)
    inner_price_SW.columns = universe  # 更换column

    inner_price = pd.concat([inner_price_HS, inner_price_SW], join='outer', axis=1)
    inner_price.to_csv(writer, 'read_index_TechData', encoding="utf-8")

    writer.save()
