import talib
import numpy as np
import pandas as pd

import pytz
from datetime import datetime

def macd(close, fastperiod=12, slowperiod=26, signalperiod=9):
    """
    计算 MACD 指标（结合 talib 和 pandas 计算方式）
    
    参数：
    - close: Series，收盘价数据
    - fastperiod: 快速 EMA 的窗口周期
    - slowperiod: 慢速 EMA 的窗口周期
    - signalperiod: 信号线 EMA 的窗口周期
    
    返回：
    - DIF: 快速线
    - DEA: 信号线
    - MACD: 柱状图数据（一般是 DIF - DEA）
    """
    # 使用 talib 计算 MACD
    dif, dea, macd = talib.MACD(close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    
    # 如果 talib 计算的结果有空值，手动补全
    if dif.isnull().any() or dea.isnull().any() or macd.isnull().any():
        # 手动计算 DIF 和 DEA
        dif_manual = close.ewm(span=fastperiod, adjust=False).mean() - close.ewm(span=slowperiod, adjust=False).mean()
        dea_manual = dif_manual.ewm(span=signalperiod, adjust=False).mean()
        macd_manual = dif_manual - dea_manual
        
        # 用手动计算的结果填充空值
        dif = dif.fillna(dif_manual)
        dea = dea.fillna(dea_manual)
        macd = macd.fillna(macd_manual)
    
    # 返回计算结果
    return dif, dea, macd

def vwap(close, volume):
    price_volume = close * volume

    # 计算累计和
    cumulative_price_volume = price_volume.cumsum()
    cumulative_volume = volume.cumsum()

    # 计算 VWAP
    return cumulative_price_volume / cumulative_volume

def volatility(close):
    log_returns = np.log(close / close.shift(1)).dropna()
    return log_returns.std()

def get_market_close_time(date=None):
    """
    获取指定日期的美股市场收盘时间（东部时间 16:00）。

    参数:
        date (optional): 指定日期，可以是 timestamp、datetime 对象、pandas.Timestamp 或字符串（yyyymmdd 或 yyyy-mm-dd 格式），
                         或 datetime.date 对象。如果为空，使用当前日期。

    返回:
        datetime: 东部时间的市场收盘时间。
    """
    # 设置东部时区
    eastern = pytz.timezone('US/Eastern')
    
    # 如果提供了 date 参数，处理它；否则使用当前时间
    if date is not None:
        # 如果 date 是 pandas.Timestamp 类型
        if isinstance(date, pd.Timestamp):
            # 如果 Timestamp 是无时区的，使用 tz_localize 设置时区
            if date.tzinfo is None:
                date = date.tz_localize(eastern)
            else:
                date = date.astimezone(eastern)  # 转为东部时间
        # 如果 date 是 datetime.datetime 对象
        elif date.__class__.__name__ == 'datetime':
            date = date.astimezone(eastern)  # 转为东部时间
        # 如果 date 是 datetime.date 对象
        elif date.__class__.__name__ == 'date':
            # 将 date 转为 datetime 对象，并设置为东部时区
            # date = datetime.combine(date, datetime.min.time()).replace(tzinfo=eastern)
            date = datetime.combine(date, datetime.min.time())
            date = eastern.localize(date)  # 强制将 datetime 设置为东部时间
        # 如果 date 是字符串，尝试解析为日期
        elif isinstance(date, str):
            # 尝试解析 yyyymmdd 格式
            try:
                date = datetime.strptime(date, '%Y%m%d')
            except ValueError:
                # 如果不成功，尝试解析 yyyy-mm-dd 格式
                date = datetime.strptime(date, '%Y-%m-%d')
            # 转为东部时间
            date = eastern.localize(date)
        # 如果 date 是 timestamp，将其转为 datetime 对象
        elif isinstance(date, (int, float)):
            date = datetime.fromtimestamp(date, tz=eastern)
        else:
            raise ValueError("date 参数必须是 timestamp、datetime、pandas.Timestamp、datetime.date 或字符串（yyyymmdd 或 yyyy-mm-dd）")
    else:
        date = datetime.now(eastern)

    # 设置指定日期的收盘时间为当天的 16:00
    market_close = date.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_close

def is_within_30_minutes_of_close(df):
    """
    Check if the market is within 30 minutes of close.

    Args:
        df (pd.DataFrame): A DataFrame containing market data with a 'date' column (datetime).

    Returns:
        bool: True if the market is within 30 minutes of close, otherwise False.
    """
    if df.empty or 'date' not in df.columns:
        raise ValueError("DataFrame is empty or does not contain a 'date' column")

    # Get the timestamp of the last row
    last_time = df.iloc[-1]['date']

    # Get the market close time (based on the first row's date to ensure consistency)
    market_close_time = get_market_close_time(df.iloc[0]['date'])

    # Check if within 30 minutes of close
    return last_time + pd.Timedelta(minutes=30) >= market_close_time