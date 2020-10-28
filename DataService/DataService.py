#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: DataService.py
@time: 2018/7/18 14:29
"""
# import sys
# sys.path.append("..")

from configparser import ConfigParser

from DataService.JydbSource import JydbSource

import datetime

class DataService(object):
    def __init__(self):
        cfg = ConfigParser()
        cfg.read('config.ini', encoding='utf-8')
        self.source = JydbSource()
        self.month_end_tradingday = []
        self.month_end_tradingday = self.get_month_end_tradingday('20000101', datetime.datetime.now().strftime('%Y%m%d'))

    def get_last_month_tradingday(self, tradingday):
        x = [s for s in self.month_end_tradingday if s <= tradingday]
        return x[-1]

    def get_month_end_tradingday(self, start, end):
        month_end_tradingday = self.source.get_month_end_tradingday(start, end)
        return month_end_tradingday


    def getTradableList(self, tradingday):
        """
        返回当天的可交易列表
        :param tradingday: '20150101'
        :return: ['600000.sh', '000001.sz']
        """
        list = self.source.get_tradableList(tradingday)
        return list

    def getTradingDay(self, start, end):
        return self.source.get_tradingday(start, end)

    def getIndexUniverse(self, tradingDay):
        """
        返回tradingday之前的index universe
        :param tradingDay: '20150101'
        :return: ['000001', '399001']
        """
        return self.source.getIndexUniverse(tradingDay)['Symbol'].map(lambda x: x.lower()).tolist()

    def getIndexETFUniverse(self, tradingDay):
        return ['510300.sh', '510050.sh', '510500.sh']

    def getIndexFutureTradable(self, tradingday):
        # supprot shfe,modify by zhaoyu
        return self.source.getIndexFutureTradable(tradingday)[['Symbol', 'exchangeCode']]

    def getUniverse(self, tradingday):
        """
        返回tradingday之前的universe
        :param tradingday: '20150101'
        :return: ['600000.sh', '000001.sz']
        """
        universe = self.source.get_universe(tradingday)
        list = universe['Symbol'].tolist()
        return list

    def getIssueType(self, tradingday):
        issuetype = self.source.get_issuetype(tradingday)
        return issuetype

    def get_universe(self, tradingday):
        universe = self.source.get_universe(tradingday)
        return universe

    def get_stock_universe_list(self, tradingday):
        universe = self.source.get_stock_universe(tradingday)
        list = universe['Symbol'].tolist()
        return list

if __name__ == '__main__':
    ds = DataService()
    # print(ds)
    td = ds.get_last_month_tradingday('20171229')
    print(td)
    # index_universe = ds.getIndexUniverse('20181214')
    # print(index_universe)

    # tradingDay = ds.getTradingDay('20150101', '20150201')
    # print(tradingDay)
    #
    # universe = ds.getUniverse('20180914')
    # print(universe)
    #
    # tradableList = ds.getTradableList('20180914')
    # print(tradableList)