import talib
import numpy as np

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