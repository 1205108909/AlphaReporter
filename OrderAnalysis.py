
# coding: utf-8


import h5py
import pandas as pd
import os.path
import numpy as np
import re
import math
import datetime
import matplotlib.pyplot as plt

tick_file = 'V:\\Data\\h5data\\stock\\tick\\'
#tick_file = 'W:\\h5data\\stock\\tick\\'
# transaction_file = 'V:\\Data\\h5data\\stock\\transaction\\'
# order_file = 'V:\\Data\\h5data\\stock\\order\\'
# orderqueue_file = 'V:\\Data\\h5data\\stock\\orderqueue\\'
# tick_file = '\\\\nas_yjs_algo\\algo_s\\Data\\h5data\\stock\\tick_new'
# index_future_path='V:\\data\\h5data\\IndexFuture\\tick\\'
# ms_path='V:\\Data\\h5data\\micro_structure\\facts21.h5'
# facts21Path = '\\\\nas_yjs_algo\\algo_s\\Data\\h5data\\micro_structure\\facts21.h5'
# daily_facts_path = '\\\\nas_yjs_algo\\algo_s\\Data\\h5data\\micro_structure\\dailyFacts.h5'
# day_bar_by_stock = 'V:\\data\\h5data\\stock\\dayBar\\byStock.h5'
# day_bar_by_date='V:\\data\\h5data\\stock\\dayBar\\byDay.h5'
# kdata_file_path = 'V:\\data\\h5data\\stock\\minuteBar\\k60s\\'
# symbol_bar_file = 'V:\\Data\\h5data\\stock\\dayBar\\byStock.h5'

def read_tick(symbol, tradingday):
    """
    read tick data
    :param symbol: '600000.sh' str
    :param tradingday: '20170104' str
    :return: pd.DataFrame
    """
    
    with h5py.File(os.path.join(tick_file, ''.join([tradingday, '.h5'])), 'r') as f:
        if symbol not in f.keys():
            return pd.DataFrame()
        time = f[symbol]['Time']
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

        tick = pd.DataFrame({'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover, 'MatchItem': matchItem, 'BSFlag': bsflag, 'AccVolume': accVolume, 'AccTurnover': accTurnover, 'AskAvgPrice': askAvgPrice, 'BidAvgPrice': bidAvgPrice, 'TotalAskVolume': totalAskVolume, 'TotalBidVolume': totalBidVolume, 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})

        for i in range(10):
            tick['BidPrice'+str(i+1)] = f[symbol]['BidPrice10'][:][:, i]
            tick['AskPrice' + str(i + 1)] = f[symbol]['AskPrice10'][:][:, i]
            tick['BidVolume' + str(i + 1)] = f[symbol]['BidVolume10'][:][:, i]
            tick['AskVolume' + str(i + 1)] = f[symbol]['AskVolume10'][:][:, i]
    return tick



def calc_vwap(symbol, date, basketId):
    symbol_length = 6 - len(symbol)
    symbol = symbol_length * '0' + symbol
    
    if  symbol.startswith('6') :
        symbol = symbol + '.sh'
    else:
        symbol = symbol + '.sz'   
    tick = read_tick(symbol, date)
    try:
#         KAFANG
        if basketId == 2:
            tick = tick[(tick['Time'] >= 144200000) & (tick['Time'] <= 145700000)]
        else:
            tick = tick[(tick['Time'] >= 140700000) & (tick['Time'] <= 144200000)]
    except Exception as e:
        print(symbol)
        return np.NaN
    return tick['Turnover'].sum()/tick['Volume'].sum()

def calc_twap(symbol, date, basketId):
    symbol_length = 6 - len(symbol)
    symbol = symbol_length * '0' + symbol
    
    if  symbol.startswith('6') :
        symbol = symbol + '.sh'
    else:
        symbol = symbol + '.sz'   
    tick = read_tick(symbol, date)
    try:
#         KAFANG
        if basketId == 2:
            tick = tick[(tick['Time'] >= 144200000) & (tick['Time'] <= 145700000)]
        else:
            tick = tick[(tick['Time'] >= 140700000) & (tick['Time'] <= 144200000)]
    except Exception as e:
        print(symbol)
        return np.NaN
    return tick['Price'].mean()

date = '20210602'

logs=pd.read_csv(f'C:\\Users\\DELL\\Desktop\\Python\\{date}\\6001044_{date}.csv', converters={'Symbol': str})
#logs=pd.read_csv(f'C:\\Users\\DELL\\Desktop\\Python\\6001044.csv', converters={'Symbol': str})
logs['BasketId'] = logs['Time'].apply(lambda t: 1 if t < 144200 else 2 )
logs['Amount'] = logs['FillPrice'] * logs['FillQty']
group=logs.groupby(['Symbol', 'BasketId'])
df=pd.DataFrame({})
df['AvgPrice'] = group['Amount'].sum()/group['FillQty'].sum()
df['Side'] = group['Side'].first()
df['Amount'] = group['Amount'].sum()
df['Trades']=group['Side'].count()

df.reset_index(inplace=True)
#df['VWAP']=df.apply(lambda t: calc_vwap(t['Symbol'], t['TradingDay'], t['BasketId']), axis=1)
df['VWAP']=df.apply(lambda t: calc_vwap(t['Symbol'], date, t['BasketId']), axis=1)
df['VWAPSlipage'] = df.apply(lambda l: (l['AvgPrice'] - l['VWAP'])/l['VWAP'] if l['Side'] == 'SELL' else -1 * (l['AvgPrice'] - l['VWAP'])/l['VWAP'], axis=1)

#df['TWAP']=df.apply(lambda t: calc_twap(t['Symbol'], t['TradingDay'], t['BasketId']), axis=1)
df['TWAP']=df.apply(lambda t: calc_twap(t['Symbol'], date, t['BasketId']), axis=1)
df['TWAPSlipage'] = df.apply(lambda l: (l['AvgPrice'] - l['TWAP'])/l['TWAP'] if l['Side'] == 'SELL' else-1 * (l['AvgPrice'] - l['TWAP'])/l['TWAP'], axis=1)

print("Amount = ",df['Amount'].sum())
weight_vwap_slipage = (df['Amount'] * df['VWAPSlipage']).sum()/df['Amount'].sum() * 10000
print("VWAPSlipage = ",weight_vwap_slipage)
weight_twap_slipage = (df['Amount'] * df['TWAPSlipage']).sum()/df['Amount'].sum() * 10000
print("TWAPSlipage = ",weight_twap_slipage)


logs[logs['Symbol'] == '601339']

tick=read_tick('002206.sz','20210527')
tick=tick[(tick['Time'] >= 140700000) & (tick['Time'] <= 144200000)]
print(tick['Price'].mean())

(5.81 - 5.817520)/ 5.817520

df.to_excel('金泰1号分析报告0602.xlsx')

df

res=df.groupby('TradingDay').apply(lambda x: calc_daily_slipage(x))
res


