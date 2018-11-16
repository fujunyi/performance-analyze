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
    # 筛选出20161001以后的交易日
    tradedays = tradedays[tradedays['tdate'] > 20161001]
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

    # if保证金
    if_positions = positions[positions['product'] == 'if'].groupby(['trade_day', 'pid', 'code'])['margin'].max()
    if_positions = if_positions.groupby(['trade_day', 'pid']).sum()
    if_positions = if_positions.reset_index()
    if_positions = if_positions.rename(columns={"margin": "if_margin"})

    # 获取balances并且合并成一个DataFrame
    balances = pd.concat(get_balance())
    balances = balances.rename(columns={"occupied": "margin"})
    # 筛选出pid不含有'_' '-' 've'的持仓
    balances = balances[~balances['pid'].str.contains('_|-|ve')]
    # 重新reset_index
    balances = balances.reset_index(drop=True)
    balances = pd.merge(balances, if_positions, on=['trade_day', 'pid'])
    balances['margin2balance'] = balances['margin']/balances['balance']
    print(balances)
