#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : Constants.py 
@Time : 2020/8/3 14:26 
"""


class Constants:
    # SignalType1 = {'ReveSignal': "TEXT LIKE '%Sell%LongHolding%' OR TEXT LIKE '%Buy%ShortHolding%'",
    #                'EndSignal': "Text like '%Normal%'", 'AggreSignal': "decisionType='22'",
    #                'SameSignalNotFirst': "TEXT LIKE '%Buy%LongHolding%' OR TEXT LIKE '%Sell%ShortHolding%'"}
    #
    # SignalType = ['Reverse', 'Close', 'First1', 'Forward', 'Normal']

    PlacementCategoryDict = {0: 'Unknown', 1: 'Aggressive', 2: 'Passive', 3: 'UltraPassive', 4: 'ClientLimit'}

    IDwithChn = {'Cld_TRX_5001093': '泰铼投资', 'Cld_ZYZC%': '中意资产'}
