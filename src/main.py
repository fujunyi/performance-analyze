import os
import re
from datetime import datetime, date
from typing import Union

import arrow
import numpy as np
import pandas as pd

DateTime = Union[datetime, date, int, str, pd.Timestamp, arrow.Arrow, np.int64]


def to_date(dt: DateTime) -> datetime:
    """
    将各种类型转变成python自带的datetime类型，取整到00:00:00
    """
    if dt is None:
        raise ValueError
    if isinstance(dt, np.datetime64):
        dt = pd.to_datetime(dt)
    if isinstance(dt, (datetime, date, pd.Timestamp, arrow.Arrow)):
        return datetime(dt.year, dt.month, dt.day)
    if isinstance(dt, str):
        dt = int(dt)
    if isinstance(dt, (int, np.int64)):
        return datetime(dt // 10000, dt // 100 % 100, dt % 100)

    raise TypeError(f"无法处理的类型:{type(dt)}")


def to_tdate(dt: DateTime) -> int:
    dt = to_date(dt)
    return dt.year * 10000 + dt.month * 100 + dt.day


def get_dbars(year: int):
    return pd.read_csv(os.path.join(filedb_path, rf"market\{year}.dbar_ftr.csv"), parse_dates=['trade_day'],
                       encoding='utf8')[['trade_day', 'code', 'close']]


def get_position():
    for day_idx in range(len(tradedays)):
        tdate = tradedays["tdate"].iloc[day_idx]
        # 获取年份
        year = int(tdate / 10000)
        # 获取文件地址
        file_path = os.path.join(filedb_path, rf'filedb\ftr_pos\{year}\{tdate}\{tdate}.ftr_pos.csv')
        # 如果文件存在，那么读取并返回文件
        if os.path.exists(file_path):
            yield pd.read_csv(file_path, parse_dates=['trade_day'])[
                ['trade_day', 'pid', 'code', 'dir', 'prev_pos', 'cur_pos']]


def get_balance():
    for day_idx in range(len(tradedays)):
        tdate = tradedays["tdate"].iloc[day_idx]
        # 获取年份
        year = int(tdate / 10000)
        # 获取文件地址
        file_path = os.path.join(filedb_path, rf'filedb\ftr_balance\{year}\{tdate}\{tdate}.ftr_balance.csv')
        # 如果文件存在，那么读取并返回文件
        if os.path.exists(file_path):
            yield pd.read_csv(file_path, parse_dates=['trade_day'])[
                ['trade_day', 'pid', 'balance', 'occupied']]


def get_pnl():
    for day_idx in range(len(tradedays)):
        tdate = tradedays["tdate"].iloc[day_idx]
        # 获取年份
        year = int(tdate / 10000)
        # 获取文件地址
        file_path = os.path.join(filedb_path, rf'filedb\daily_pnl\{year}\{tdate}\{tdate}.daily_pnl.csv')
        # 如果文件存在，那么读取并返回文件
        if os.path.exists(file_path):
            yield pd.read_csv(file_path, parse_dates=['trade_day'])[
                ['trade_day', 'pid', 'code', 'profit', 'fee']]


if __name__ == '__main__':
    filedb_path = r"\\fatman\data\broker"
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.width', 200)

    # 获取tradeday交易日
    tradedays = pd.read_csv(os.path.join(filedb_path, r"market\tradeday.csv"), parse_dates=['trade_day'])

    # 获取multiple 合约乘数， 只需要用到code和合约乘数
    symbol_infos = pd.read_csv(os.path.join(filedb_path, r"market\symbol_info.csv"))[['code', 'multiple']]

    # 获取margin_ratio 保证金比率， 只需要用到product和保证金比率
    product_infos = pd.read_csv(os.path.join(filedb_path, r"market\product_infos.csv"))[['product', 'margin_ratio']]

    # 获取dbar 日线，只需要用到交易日，code和收盘价
    dbars = pd.concat([get_dbars(2016), get_dbars(2017), get_dbars(2018)])

    # 筛选出上海证券交易所的交易日
    tradedays = tradedays.loc[(tradedays['product'] == 'busyhour') & (tradedays['exchange'] == 'SESH')]

    # 获取交易日的int型
    tradedays['tdate'] = tradedays.apply(lambda x: to_tdate(x["trade_day"]), axis=1)

    # 筛选出20180101以后的交易日
    tradedays = tradedays[tradedays['tdate'] > 20180101]

    # print(tradedays)
    # os._exit(0)

    # 获取所有的positions并且合并成一个DataFrame
    positions = pd.concat(get_position())

    # 筛选出pid不含有'_' '-' 've'的持仓
    positions = positions[~positions['pid'].str.contains('_|-|ve')]

    # 重新reset_index
    positions = positions.reset_index(drop=True)

    # 获取product 品种
    positions['product'] = positions.apply(lambda x: re.search('[a-zA-Z]+', x['code']).group(0), axis=1)

    # 获取multiple，合约乘数
    positions = pd.merge(positions, symbol_infos, on='code')
    # 获取margin_ratio
    positions = pd.merge(positions, product_infos, on='product')
    # 获取每日收盘价
    positions = pd.merge(positions, dbars, on=['trade_day', 'code'])

    # 获取总持仓
    positions['total_pos'] = positions['prev_pos'] + positions['cur_pos']
    # 获取合约价值
    positions['value'] = positions['total_pos'] * positions['multiple'] * positions['close']
    # 获取保证金
    positions['margin'] = positions['value'] * positions['margin_ratio']
    # 排序
    positions = positions.sort_values(['trade_day', 'pid', 'code', 'dir'])
    positions: pd.DataFrame = positions.reset_index(drop=True)

    # positions:    trade_day    pid    code    dir    prev_pos    cur_pos    product    multiple    margin_ratio    close    total_pos    value    margin
    # positions.to_csv('posrtions.csv')

    # 获取balances并且合并成一个DataFrame
    balances = pd.concat(get_balance())
    balances = balances.rename(columns={"occupied": "margin"})
    # 筛选出pid不含有'_' '-' 've'的持仓
    balances = balances[~balances['pid'].str.contains('_|-|ve')]
    # 重新reset_index
    balances = balances.reset_index(drop=True)

    # balances:   trade_day      pid     balance      margin

    # 获取daily_pnl
    pnl = pd.concat(get_pnl())
    # 筛选出pid不含有'_' '-' 've'的持仓, na=False? there are NaN values?
    pnl = pnl[~pnl['pid'].str.contains('_|-|ve', na=False)]
    pnl = pnl.reset_index(drop=True)
    pnl = pnl[['trade_day', 'pid', 'profit', 'fee']].groupby(['trade_day', 'pid']).sum().reset_index()
    balances = pd.merge(balances, pnl, on=['trade_day', 'pid'])
    # 获取基金单位净值
    balances = balances.loc[balances.balance != 0]
    balances['net_profit'] = (balances['profit'] - balances['fee']) / balances['balance'] + 1
    # balances['net_profit'] = balances['net_profit'].cumsum()

    balances: pd.DataFrame = balances.to_csv('balances.csv')
    exit()

    # 获取股指期货保证金占比
    stock_index_margin = positions.loc[
        (positions['product'] == 'if') | (positions['product'] == 'ih') | (positions['product'] == 'ic')].groupby(
        ['trade_day', 'pid', 'code'])['margin'].max().groupby(['trade_day', 'pid']).sum()
    stock_index_margin = stock_index_margin.reset_index()
    stock_index_margin = stock_index_margin.rename(columns={"margin": "stock_index_margin"})
    balances = pd.merge(balances, stock_index_margin, on=['trade_day', 'pid'])
    balances['stock_index_proportion'] = balances['stock_index_margin'] / balances['balance']

    # 获取商品期货保证金占比
    commodity_position = positions.loc[
        ~(positions['product'] == 'if') & ~(positions['product'] == 'ih') & ~(positions['product'] == 'ic')]
    commodity_margin = commodity_position.groupby(['trade_day', 'pid', 'code'])['margin'].max().groupby(
        ['trade_day', 'pid']).sum().reset_index()
    commodity_margin = commodity_margin.rename(columns={"margin": "commodity_margin"})
    balances = pd.merge(balances, commodity_margin, on=['trade_day', 'pid'])
    balances['commodity_proportion'] = balances['commodity_margin'] / balances['balance']

    # 商品期货合约价值(不扎差not net）commodity_nn_value
    commodity_nn_value = commodity_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    commodity_nn_value = commodity_nn_value.rename(columns={"value": "commodity_nn_value"})
    balances = pd.merge(balances, commodity_nn_value, on=['trade_day', 'pid'])

    # 商品期货合约价值（扎差net）commodity_net_value
    commodity_long_position = commodity_position.loc[(commodity_position['dir'] == 'Long')]
    commodity_long_value = commodity_long_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    commodity_long_value = commodity_long_value.rename(columns={"value": "commodity_long_value"})
    balances = pd.merge(balances, commodity_long_value, on=['trade_day', 'pid'])

    commodity_short_position = commodity_position.loc[(commodity_position['dir'] == 'Short')]
    commodity_short_value = commodity_short_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    commodity_short_value = commodity_short_value.rename(columns={"value": "commodity_short_value"})
    balances = pd.merge(balances, commodity_short_value, on=['trade_day', 'pid'])

    balances['commodity_net_value'] = balances['commodity_long_value'] - balances['commodity_short_value']

    # IF合约价值
    if_long_position = positions.loc[(positions['product'] == 'if') & (positions['dir'] == 'Long')]
    if_long_value = if_long_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    if_long_value = if_long_value.rename(columns={"value": "if_long_value"})
    balances = pd.merge(balances, if_long_value, on=['trade_day', 'pid'])

    if_short_position = positions.loc[(positions['product'] == 'if') & (positions['dir'] == 'Short')]
    if_short_value = if_short_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    if_short_value = if_short_value.rename(columns={"value": "if_short_value"})
    balances = pd.merge(balances, if_short_value, on=['trade_day', 'pid'])

    balances['if_value'] = balances['if_long_value'] - balances['if_short_value']

    # IH合约价值
    ih_long_position = positions.loc[(positions['product'] == 'ih') & (positions['dir'] == 'Long')]
    ih_long_value = ih_long_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    ih_long_value = ih_long_value.rename(columns={"value": "ih_long_value"})
    balances = pd.merge(balances, ih_long_value, on=['trade_day', 'pid'])

    ih_short_position = positions.loc[(positions['product'] == 'ih') & (positions['dir'] == 'Short')]
    ih_short_value = ih_short_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    ih_short_value = ih_short_value.rename(columns={"value": "ih_short_value"})
    balances = pd.merge(balances, ih_short_value, on=['trade_day', 'pid'])

    balances['ih_value'] = balances['ih_long_value'] - balances['ih_short_value']

    # IC合约价值
    ic_long_position = positions.loc[(positions['product'] == 'ic') & (positions['dir'] == 'Long')]
    ic_long_value = ic_long_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    ic_long_value = ic_long_value.rename(columns={"value": "ic_long_value"})
    balances = pd.merge(balances, ic_long_value, on=['trade_day', 'pid'])

    ic_short_position = positions.loc[(positions['product'] == 'ic') & (positions['dir'] == 'Short')]
    ic_short_value = ic_short_position[['trade_day', 'pid', 'value']].groupby(['trade_day', 'pid']).sum()
    ic_short_value = ic_short_value.rename(columns={"value": "ic_short_value"})
    balances = pd.merge(balances, ic_short_value, on=['trade_day', 'pid'])

    balances['ic_value'] = balances['ic_long_value'] - balances['ic_short_value']

    # 最终结果写出到balances.csv
    balances: pd.DataFrame = balances.to_csv('balances.csv')
    exit()
