#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : StatSymbolLiquidityAndVolatility.py 
@Time : 2021/1/8 8:53
1. 获取clientOrder by symbol, 21年后， 20年12月

2.流动性分析（算法同ScreenUniv）
 a.统计市场所有股票的流动性
 b.分别统计客户交易股票、成交额排名50%以上的股票，在流动性排名的中值

3.波动性分析（highPx - lowPx)/preClosePx）
 a.统计市场所有股票的波动性
 b.分别统计客户交易股票、成交额排名50%以上的股票，在波动性排名的中值
"""

import os

import numpy as np
import pandas as pd
import pymssql

import Log
from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


class StatSymbolLiquidityAndVolatility(object):
    def __init__(self, start, end, clientId):
        self.logger = Log.get_logger(__name__)
        self.server_atr = "172.10.10.7"
        self.database_atr = "AlgoTradeReport"
        self.user_atr = "algodb"
        self.password_atr = "!AlGoTeAm_"
        self.conn_atr = None

        self.server_jy = "172.10.10.9"
        self.user_jy = "jydb1"
        self.password_jy = "jydb1"
        self.database_jy = "jydb1"
        self.conn_jy = None

        self.jyloader = JYDataLoader()
        self.run(start, end, clientId)

    def get_atr_connection(self):
        try:
            self.conn_atr = pymssql.connect(self.server_atr, self.user_atr, self.password_atr, self.database_atr)
            return self.conn_atr
        except pymssql.OperationalError as e:
            print(e)

    def get_jy_connection(self):
        try:
            self.conn_jy = pymssql.connect(self.server_jy, self.user_jy, self.password_jy, self.database_jy)
            return self.conn_jy
        except pymssql.OperationalError as e:
            print(e)

    # def get_stocks(self, isAlive=1):
    #     secuCodes = []
    #     mdSymbols = []
    #     innerCodes = []
    #     secuMarkets = []
    #     secuAbbrs = []
    #     listedDates = []
    #     symbols = []
    #     with self.get_jy_connection() as conn:
    #         with conn.cursor(as_dict=True) as cursor:
    #             proc = 'spu_GetAStockAll'
    #             cursor.callproc(proc, (isAlive,))
    #             for row in cursor:
    #                 secuCodes.append(row['secuCode'])
    #                 mdSymbols.append(row['stockSymbol'])
    #                 innerCodes.append(row['InnerCode'])
    #                 secuMarkets.append(row['secuMarket'])
    #                 secuAbbrs.append(row['SecuAbbr'])
    #                 listedDates.append(row['ListedDate'])
    #                 symbols.append(row['stockSymbol_1'])
    #
    #     df = pd.DataFrame({'stockcode': secuCodes,
    #                        'mdSymbol': mdSymbols,
    #                        'innerCode': innerCodes,
    #                        'secuMarket': secuMarkets,
    #                        'secuAbbr': secuAbbrs,
    #                        'listedDate': listedDates, 'symbol': symbols})
    #     return df

    def get_stocks_without_star(self, isAlive=1):
        mdSymbols = []
        innerCodes = []
        secuMarkets = []
        with self.get_jy_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetAStock'
                cursor.callproc(proc, (isAlive,))
                for row in cursor:
                    if row['stockSymbol'].startswith('688'):
                        continue
                    mdSymbols.append(row['stockSymbol'])
                    innerCodes.append(row['InnerCode'])
                    secuMarkets.append(row['secuMarket'])

        df = pd.DataFrame({'symbol': mdSymbols, 'innerCode': innerCodes, 'secuMarkets': secuMarkets})
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
            with self.get_jy_connection() as conn:
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

    def get_trade_symbol(self, start, end, clientId):
        """
        get_clientOrder
        :param start: '20150101'
        :param end: '20150130'
        :return: df :
        """
        symbol = []
        turnover = []
        slipage = []
        with self.get_atr_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = f"SELECT symbol, SUM (avgPrice * cumQty) as turnover, SUM (avgPrice * cumQty * slipageInBps)/SUM (avgPrice * cumQty) as slipage FROM ClientOrder WHERE clientId = \'{clientId}\' AND tradingDay >= \'{start}\' AND tradingDay <= \'{end}\' AND cumQty > 0 GROUP BY symbol ORDER BY SUM (avgPrice * cumQty) DESC"
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['symbol'])
                    turnover.append(row['turnover'])
                    slipage.append(row['slipage'])
        data = pd.DataFrame({'symbol': symbol, 'turnover': turnover, 'slipage': slipage})
        data['turnover'] = data['turnover'].astype('float')
        data['slipage'] = data['slipage'].astype('float')
        return data

    def get_TechDatas(self, tradingdays, innerCodes, code):
        return self.jyloader.read_price(tradingdays, innerCodes, code)

    def get_AShare(self, tradingdays, innerCodes, code):
        return self.jyloader.read_share(tradingdays, innerCodes, code)

    def run(self, start, end, clientId):
        df_tradingDay = self.get_tradingday(start, end)
        df_stocks = self.get_stocks_without_star(1)
        df_trade_symbol = self.get_trade_symbol(start, end, clientId)

        closep = self.get_TechDatas(df_tradingDay, df_stocks, 'PC')
        closep.fillna(0, inplace=True)
        closep.columns = df_stocks['symbol']

        highPx = self.get_TechDatas(df_tradingDay, df_stocks, 'PH')
        highPx.fillna(0, inplace=True)
        highPx.columns = df_stocks['symbol']

        lowPx = self.get_TechDatas(df_tradingDay, df_stocks, 'PL')
        lowPx.fillna(0, inplace=True)
        lowPx.columns = df_stocks['symbol']

        openPx = self.get_TechDatas(df_tradingDay, df_stocks, 'PO')
        openPx.fillna(0, inplace=True)
        openPx.columns = df_stocks['symbol']

        preClosePx = self.get_TechDatas(df_tradingDay, df_stocks, 'PPC')
        preClosePx.fillna(0, inplace=True)
        preClosePx.columns = df_stocks['symbol']

        aShareFloat = self.get_AShare(df_tradingDay, df_stocks, 'NAS')
        aShareFloat.fillna(0, inplace=True)
        aShareFloat.columns = df_stocks['symbol']

        vol = self.get_TechDatas(df_tradingDay, df_stocks, 'VM')
        vol.fillna(0, inplace=True)
        vol.columns = df_stocks['symbol']

        fcap = aShareFloat * closep
        amount = vol * closep
        fcap_avg = fcap.mean()
        amount_avg = amount.mean()
        fcap_rank = fcap_avg.rank(method='average').astype(int)
        amount_rank = amount_avg.rank(method='average').astype(int)

        univ_liq = fcap_rank * 0.5 + amount_rank * 0.5
        univ_liq_rank = univ_liq.rank(method='average').astype(np.float32)
        univ_liq_rank_normal = round(univ_liq_rank / univ_liq_rank.shape[0], 4)

        df_preClosePx_notzero = preClosePx[preClosePx != 0]
        df_preClosePx_notzero.fillna(method='ffill', inplace=True)
        df_preClosePx_notzero.fillna(method='bfill', inplace=True)

        df_vix = (highPx - lowPx).div(df_preClosePx_notzero) * 100
        df_vix_notzero = df_vix[df_vix != 0]
        df_vix_notzero.fillna(method='ffill', inplace=True)
        df_vix_notzero.fillna(method='bfill', inplace=True)

        univ_vix = df_vix_notzero.mean()
        univ_vix_rank = univ_vix.rank(method='min').astype(np.float32)
        univ_vix_rank_normal = round(univ_vix_rank / univ_vix_rank.shape[0], 4)

        df_univ_rank_normal = pd.DataFrame({'liq_rank': univ_liq_rank_normal, 'vix_rank': univ_vix_rank_normal})
        df_trade_symbol = df_trade_symbol.merge(df_univ_rank_normal, left_on='symbol', right_index=True,
                                                how='left')

        fileName = f'liquidity_volatility_{start}_{end}_({clientId}).csv'
        pathCsv = os.path.join(f'Data/{fileName}')

        df_trade_symbol.to_csv(pathCsv)
        self.logger.info(f'calculator: {start}_{end}_{clientId} successfully')


if __name__ == '__main__':
    start = '20210104'
    end = '20210111'
    clientId = 'Cld_TRX_5001016'
    reporter = StatSymbolLiquidityAndVolatility(start, end, clientId)
