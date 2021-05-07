import datetime
import os
import sys
import zipfile

import h5py
import numpy as np
import pandas as pd
import pymssql

import Log
from Constants import OrderReportID
from DataSender.EmailHelper import EmailHelper
from DataSender.ExcelHelper import ExcelHelper
from DataService.JYDataLoader import JYDataLoader

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)

from enum import Enum


class IdType(Enum):
    clientId = 1
    accountId = 2


class OrderReporter(object):
    def __init__(self, start, end):
        self.logger = Log.get_logger(__name__)
        self.tick_path = "Y:/Data/h5data/stock/tick/"
        self.server = "172.10.10.7"
        self.database = "AlgoTradeReport"
        self.user = "algodb"
        self.password = "!AlGoTeAm_"
        self.conn = None

        jyloader = JYDataLoader()
        tradingdays = jyloader.get_tradingday(start, end)
        clientIds = OrderReportID.clientId
        accountIds = OrderReportID.accountId
        zipClientIds = OrderReportID.zipClientId
        self.email = EmailHelper.instance()
        one_2_one_id = {IdType.clientId: clientIds, IdType.accountId: accountIds}
        one_2_mul_id = {IdType.clientId: zipClientIds}
        self.run(tradingdays, one_2_one_id, one_2_mul_id)

    def get_connection(self):
        try:
            self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
            return self.conn
        except pymssql.OperationalError as e:
            print(e)

    def get_receiveList(self, id_type, id):
        if id == '':
            return pd.DataFrame()
        accountIds = []
        clientIds = []
        clientName = []
        to_receiver = []
        cc_receiver = []
        id = id + '%'
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                if id_type == IdType.clientId:
                    stmt = f"select * from ClientsForPy where clientId like \'{id}\'"
                else:
                    stmt = f"select * from ClientsForPy where accountId like \'{id}\'"
                cursor.execute(stmt)
                for row in cursor:
                    accountIds.append(row['accountId'])
                    clientIds.append(row['clientId'])
                    clientName.append(row['clientName'])
                    to_receiver.append(row['email'])
                    cc_receiver.append(row['repsentEmail'])

        data = pd.DataFrame(
            {'accountId': accountIds, 'clientId': clientIds, 'clientName': clientName, 'to_receiver': to_receiver,
             'cc_receiver': cc_receiver})
        return data

    def get_clientOrder(self, tradingday, id_type, id):
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
                if id_type == IdType.clientId:
                    stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and clientId like \'{id}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
                else:
                    stmt = f"select * from ClientOrderView where orderQty>0 and (securityType='RPO' or securityType='EQA') and tradingDay = \'{tradingday}\' and accountId like \'{id}\' AND algo <> 'POV' AND algo <> 'PEGGING'"
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

    def get_client_order_count(self, tradingday, id_type, id):
        orderId = []
        sliceStatus = []
        sliceCount = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                if id_type == IdType.clientId:
                    stmt = f"SELECT a.orderId, sliceStatus, sliceCount FROM ClientOrderView a JOIN (SELECT orderId,orderStatus AS sliceStatus,COUNT (*) AS sliceCount FROM ExchangeOrderView WHERE orderStatus IN ('Filled', 'Canceled') GROUP BY orderId,orderStatus) b ON a.orderId = b.orderId WHERE a.tradingDay = \'{tradingday}\' AND a.clientId like \'{id}\' AND a.algo <> 'POV' AND a.algo <> 'PEGGING'  ORDER BY a.orderId"
                else:
                    stmt = f"SELECT a.orderId, sliceStatus, sliceCount FROM ClientOrderView a JOIN (SELECT orderId,orderStatus AS sliceStatus,COUNT (*) AS sliceCount FROM ExchangeOrderView WHERE orderStatus IN ('Filled', 'Canceled') GROUP BY orderId,orderStatus) b ON a.orderId = b.orderId WHERE a.tradingDay = \'{tradingday}\' AND a.algo <> 'POV' AND a.algo <> 'PEGGING'  AND a.accountId like \'{id}\' ORDER BY a.orderId"
                cursor.execute(stmt)
                for row in cursor:
                    orderId.append(row['orderId'])
                    sliceStatus.append(row['sliceStatus'])
                    sliceCount.append(row['sliceCount'])

        data = pd.DataFrame({'orderId': orderId, 'sliceStatus': sliceStatus, 'sliceCount': sliceCount})
        data['sliceCount'] = data['sliceCount'].astype('int')
        return data

    def get_exchangeOrder(self, tradingday, id_type, id):
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
                if id_type == IdType.clientId:
                    stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.clientId like \'{id}\' AND b.algo <> 'POV' AND b.algo <> 'PEGGING'"
                else:
                    stmt = f"SELECT a.sliceId, a.orderId, b.side,b.symbol,a.effectiveTime,a.qty,a.cumQty,a.leavesQty,a.price,a.sliceAvgPrice,a.orderStatus from ExchangeOrderView a join ClientOrderView b on a.orderId=b.orderId where a.orderStatus in ('Filled','Canceled') AND b.tradingDay = \'{tradingday}\' AND b.accountId like \'{id}\' AND b.algo <> 'POV' AND b.algo <> 'PEGGING'"
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

    def read_symbol_tick(self, tradingday, symbol):
        with h5py.File(os.path.join(self.tick_path + tradingday + ".h5"), 'r') as h5file:
            if symbol in h5file.keys():
                df_tick_symbol = pd.DataFrame({'Time': h5file[symbol]['Time'][:],
                                               'Price': h5file[symbol]['Price'][:],
                                               'AccTurnover': h5file[symbol]['AccTurnover'][:],
                                               'AccVolume': h5file[symbol]['AccVolume'][:],
                                               'Volume': h5file[symbol]['Volume'][:],
                                               'BSFlag': h5file[symbol]['BSFlag'][:],
                                               'BidAvgPrice': h5file[symbol]['BidAvgPrice'][:],
                                               'High': h5file[symbol]['High'][:],
                                               'Low': h5file[symbol]['Low'][:],
                                               'MatchItem': h5file[symbol]['MatchItem'][:],
                                               'Open': h5file[symbol]['Open'][:],
                                               'PreClose': h5file[symbol]['PreClose'][:],
                                               'TotalAskVolume': h5file[symbol]['TotalAskVolume'][:],
                                               'TotalBidVolume': h5file[symbol]['TotalBidVolume'][:],
                                               'Turnover': h5file[symbol]['Turnover'][:],
                                               'AskAvgPrice': h5file[symbol]['AskAvgPrice'][:]})
                return df_tick_symbol
            else:
                self.logger.warn("there is no TickData (" + symbol + ") in h5 file, please check your data")
                return pd.DataFrame()

    def get_tick_by_symbol(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        df_tick_symbol = self.read_symbol_tick(tradingDay, symbol)
        if df_tick_symbol.shape[0] == 0:
            return pd.DataFrame()
        if price == 0:
            return df_tick_symbol[(df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                    df_tick_symbol['Volume'] > 0)]
        else:
            if side == 'Buy' or side == 1:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0)]
            else:
                return df_tick_symbol[
                    (df_tick_symbol['Time'] >= startTime) & (df_tick_symbol['Time'] <= endTime) & (
                            df_tick_symbol['Volume'] > 0)]

    def get_twap(self, tradingDay, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        data = self.get_tick_by_symbol(tradingDay, symbol, startTime, endTime, price, side)
        return data.Price.sum() / data.Volume.count() if data.size > 0 else 0

    def stat_summary(self, df, side, field):
        df = df[(df['side'] == side) & (df[field] != 0)]
        amt = sum(df['turnover'])
        if side == 'Buy':
            slipage = 0 if amt == 0 else sum((df[field] - df['avgPrice']) / df[field] * df['turnover']) / sum(
                df['turnover'])
        else:
            slipage = 0 if amt == 0 else sum((df['avgPrice'] - df[field]) / df[field] * df['turnover']) / sum(
                df['turnover'])
        pnl_yuan = slipage * amt
        return amt, slipage, pnl_yuan

    def compress_file(self, folderPath, compressPathName):
        '''
        :param folderPath: 文件夹路径
        :param compressPathName: 压缩包路径
        '''
        zip_full_file = os.path.join(folderPath, compressPathName)
        with zipfile.ZipFile(zip_full_file, 'w') as zip:
            for dirpath, dirNames, fileNames in os.walk(folderPath):
                fpath = dirpath.replace(folderPath, '')  # 这一句很重要，不replace的话，就从根目录开始复制
                fpath = fpath and fpath + os.sep or ''  # 这句话理解我也点郁闷，实现当前文件夹以及包含的所有文件的压缩
                for filename in fileNames:
                    if not filename.endswith('xlsx'):
                        continue
                    zip.write(os.path.join(dirpath, filename), fpath + filename)

    def run(self, tradingDays, dict_ids, one_2_mul_id):
        def cal_twap(tradingDay, effectiveTime, expireTime, symbol, price, side, cumQty):
            if cumQty == 0:
                return 0
            effectiveTime = effectiveTime.hour * 10000000 + effectiveTime.minute * 100000 + effectiveTime.second * 1000
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_twap-{tradingDay}-{effectiveTime}-{expireTime}-{symbol}-{price}-{side}')
            twap = self.get_twap(tradingDay, symbol, effectiveTime, expireTime, price, side)
            return twap

        def cal_twap_slipage(twap, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByTwap = 0.00 if twap == 0.00 else (
                (avgprice - twap) / twap if side == 'Sell' else (twap - avgprice) / twap)
            return slipageByTwap

        def cal_ocp(tradingDay, expireTime, symbol, cumQty):
            if cumQty == 0: return 0
            expireTime = expireTime.hour * 10000000 + expireTime.minute * 100000 + expireTime.second * 1000
            self.logger.info(f'cal_ocp-{tradingDay}-{expireTime}-{symbol}')
            tick = read_tick(symbol, tradingDay)
            if tick is None:
                return None
            tick = tick[tick['Time'] <= expireTime]
            return tick.tail(1).iloc[0, :]['Price']

        def cal_ocp_slipage(ocp, side, avgprice):
            avgprice = np.float64(avgprice)
            slipageByOCP = 0.00 if ocp == 0.00 else (
                (avgprice - ocp) / ocp if side == 'Sell' else (ocp - avgprice) / ocp)
            return slipageByOCP

        def read_tick(symbol, tradingday):
            """
            read tick data
            :param symbol: '600000.sh' str
            :param tradingday: '20170104' str
            :return: pd.DataFrame Time类型：93003000 int
            """
            with h5py.File(os.path.join(self.tick_path, ''.join([tradingday, '.h5'])), 'r') as f:
                if symbol not in f.keys():
                    return None
                    # raise Exception(f'{tradingday}_{symbol} tick 为空')
                time = f[symbol]['Time']
                if len(time) == 0:
                    raise Exception(f'{tradingday}_{symbol} tick 为空')
                price = f[symbol]['Price']
                volume = f[symbol]['Volume']
                turnover = f[symbol]['Turnover']
                tick = pd.DataFrame(
                    {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover})

            return tick

        def cal_client_exchange_summary(tradingDay, id_type, id):
            df_client_order = self.get_clientOrder(tradingDay, id_type, id)
            if len(df_client_order) == 0:
                return False
            df_client_order_count = self.get_client_order_count(tradingDay, id_type, id)
            df_client_order_count['sliceStatus'] = df_client_order_count['sliceStatus'].map(
                lambda x: x + 'Count')
            df_client_order_count = df_client_order_count.pivot(index='orderId', columns='sliceStatus',
                                                                values='sliceCount')
            df_client_order = df_client_order.merge(df_client_order_count, how='left', left_on='orderId',
                                                    right_index=True)
            df_client_order.fillna(0, inplace=True)
            df_client_order.sort_values(by=['effectiveTime'], inplace=True)

            # 调整列顺序
            VWAPs = df_client_order.pop('VWAP')
            df_client_order.insert(df_client_order.shape[1], 'VWAP', VWAPs)
            slipageByVWAPs = df_client_order.pop('slipageByVWAP')
            df_client_order.insert(df_client_order.shape[1], 'slipageByVWAP', slipageByVWAPs)

            # 1.计算twap
            df_client_order['TWAP'] = df_client_order.apply(
                lambda x: cal_twap(tradingDay, x['effectiveTime'], x['expireTime'], x['symbol'], x['avgPrice'],
                                   x['side'], x['cumQty']), axis=1)

            # 2.计算slipageByTwap
            df_client_order['slipageByTWAP'] = df_client_order.apply(
                lambda x: cal_twap_slipage(x['TWAP'], x['side'], x['avgPrice']), axis=1)
            df_client_order.loc[df_client_order.loc[:, 'cumQty'] == 0, ['slipageByTWAP']] = 0.00
            df_client_order['TWAP'] = round(df_client_order['TWAP'], 5)
            df_client_order['slipageByTWAP'] = round(df_client_order['slipageByTWAP'] * 10000, 2)

            # 3.计算OrderClosePx
            df_client_order['OCP'] = df_client_order.apply(
                lambda x: cal_ocp(tradingDay, x['expireTime'], x['symbol'], x['cumQty']), axis=1)
            df_client_order['slipageByOCP'] = df_client_order.apply(
                lambda x: cal_ocp_slipage(x['OCP'], x['side'], x['avgPrice']), axis=1)
            df_client_order['slipageByOCP'] = round(df_client_order['slipageByOCP'] * 10000, 2)

            df_exchange_order = self.get_exchangeOrder(tradingday=tradingDay, id_type=id_type, id=id)

            buy_amt, buy_vwap_slipage, buy_pnl_vwap_yuan = self.stat_summary(df_client_order, 'Buy', 'VWAP')
            buy_amt, buy_twap_slipage, buy_pnl_twap_yuan = self.stat_summary(df_client_order, 'Buy', 'TWAP')
            buy_amt, buy_ocp_slipage, buy_pnl_ocp_yuan = self.stat_summary(df_client_order, 'Buy', 'OCP')

            sell_amt, sell_vwap_slipage, sell_pnl_vwap_yuan = self.stat_summary(df_client_order, 'Sell', 'VWAP')
            sell_amt, sell_twap_slipage, sell_pnl_twap_yuan = self.stat_summary(df_client_order, 'Sell', 'TWAP')
            sell_amt, sell_ocp_slipage, sell_pnl_ocp_yuan = self.stat_summary(df_client_order, 'Sell', 'OCP')

            df_summary = pd.DataFrame(
                {'Side': ['Buy', 'Sell'],
                 'FilledAmt(万元)': [round(buy_amt / 10000, 3), round(sell_amt / 10000, 3)],
                 'PnL2VWAP(BPS)': [round(buy_vwap_slipage * 10000, 2),
                                   round(sell_vwap_slipage * 10000, 2)],
                 'PnL2TWAP(BPS)': [round(buy_twap_slipage * 10000, 2),
                                   round(sell_twap_slipage * 10000, 2)],
                 'PnL2OCP(BPS)': [round(buy_ocp_slipage * 10000, 2),
                                  round(sell_ocp_slipage * 10000, 2)],
                 'PnL2VWAP(YUAN)': [round(buy_pnl_vwap_yuan, 2),
                                    round(sell_pnl_vwap_yuan, 2)],
                 'PnL2TWAP(YUAN)': [round(buy_pnl_twap_yuan, 2),
                                    round(sell_pnl_twap_yuan, 2)],
                 'PnL2OCP(YUAN)': [round(buy_pnl_ocp_yuan, 2),
                                   round(sell_pnl_ocp_yuan, 2)]
                 }, index=[1, 2])

            if (len(df_exchange_order) == 0) & (len(df_summary) == 0):
                return False

            ExcelHelper.createExcel(pathCsv)
            df_client_order['effectiveTime'] = df_client_order['effectiveTime'].map(
                lambda x: x.strftime('%H:%M:%S'))
            df_client_order['expireTime'] = df_client_order['expireTime'].map(lambda x: x.strftime('%H:%M:%S'))
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_summary, header=True,
                                           sheet_name='algoSummary', sep_key='all_name')
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_client_order,
                                           header=True, sheet_name='algoClientOrder', sep_key='all_name')
            ExcelHelper.Append_df_to_excel(file_name=pathCsv, df=df_exchange_order,
                                           header=True, sheet_name='algoExchangeOrder', sep_key='all_name')
            ExcelHelper.removeSheet(pathCsv, 'Sheet')
            self.logger.info(f'calculator: {tradingDay}__{id} successfully')
            return True

        for tradingDay in tradingDays:
            if not os.path.exists(os.path.join(self.tick_path, tradingDay + '.h5')):
                self.logger.error(f'{tradingDay} h5 tick is not existed.')
                continue
            dir_data = os.path.join(f'Data/OrderReporter/{tradingDay}')
            if not os.path.exists(dir_data):
                os.makedirs(dir_data)

            for id_type, ids in dict_ids.items():
                for id in ids:
                    df_receive = self.get_receiveList(id_type=id_type, id=id)
                    df_receive['tradingDay'] = tradingDay
                    self.logger.info(f'start calculator: {tradingDay}__{id_type}__{id}')
                    fileName = f'OrderReporter_{tradingDay}_({id}).xlsx'
                    pathCsv = os.path.join(f'Data/OrderReporter/{tradingDay}/{fileName}')

                    isSuccess = cal_client_exchange_summary(tradingDay, id_type, id)
                    if isSuccess:
                        self.email.add_email_content(f'ClientOrderReporter_{tradingDay}_({id})交易报告，请查收')
                        self.email.send_email_file(pathCsv, fileName, df_receive, id, subject_prefix='OrderReporter')
                        self.email.content = ''
                        self.logger.info(f'calculator: {tradingDay}__{id} successfully')

        for id_type, zip_ids_dict in one_2_mul_id.items():
            for email_key, zip_ids in zip_ids_dict.items():
                if len(zip_ids) == 0:
                    continue
                df_receive = self.get_receiveList(id_type=id_type, id=email_key)
                df_receive['tradingDay'] = tradingDay
                self.email.add_email_content(f'ClientOrderReporter_{tradingDay}_({email_key})交易报告，请查收')
                dir_csv = os.path.join(dir_data, f'{email_key}')
                if not os.path.exists(dir_csv):
                    os.makedirs(dir_csv)
                for id in zip_ids:
                    self.logger.info(f'start calculator: {tradingDay}__{id_type}__{id}')
                    fileName = f'OrderReporter_{tradingDay}_({id}).xlsx'
                    pathCsv = os.path.join(dir_csv, fileName)
                    cal_client_exchange_summary(tradingDay, id_type, id)
                  
                self.compress_file(dir_csv, f'{email_key}.zip')
                zip_file = os.path.join(dir_csv, f'{email_key}.zip')
                self.email.send_email_zip(zip_file, f'{email_key}.zip', df_receive, subject_prefix='OrderReporter')
                self.email.content = ''
                self.logger.info(f'send_email_file: {tradingDay}__{email_key} successfully')


if __name__ == '__main__':
    start = sys.argv[1]
    end = sys.argv[2]
    reporter = OrderReporter(start, end)
