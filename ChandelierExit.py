import pandas as pd
from collections import deque

class ChandelierExit:
    def __init__(self, period=22, k=3.0):
        self.period = period
        self.k = k
        self.prev_close = None
        self.prev_atr = None
        self.high_window = deque(maxlen=period)
        self.low_window = deque(maxlen=period)

        self._atr_list = []
        self._long_list = []
        self._short_list = []

    def compute_tr(self, high, low, prev_close):
        return max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

    def update(self, row: pd.Series):
        high = row['high']
        low = row['low']
        close = row['close']

        # 计算 TR
        if self.prev_close is None:
            tr = high - low
        else:
            tr = self.compute_tr(high, low, self.prev_close)

        # 计算 ATR
        if self.prev_atr is None:
            atr = tr
        else:
            atr = (self.prev_atr * (self.period - 1) + tr) / self.period

        self.prev_close = close
        self.prev_atr = atr
        self._atr_list.append(atr)

        # 更新最高最低窗口
        self.high_window.append(high)
        self.low_window.append(low)

        # 计算 Chandelier 值（不足 period 返回 None）
        if len(self.high_window) == self.period:
            highest_high = max(self.high_window)
            lowest_low = min(self.low_window)
            chandelier_long = highest_high - self.k * atr
            chandelier_short = lowest_low + self.k * atr
        else:
            chandelier_long = None
            chandelier_short = None

        self._long_list.append(chandelier_long)
        self._short_list.append(chandelier_short)

    @property
    def atr_series(self):
        return pd.Series(self._atr_list)

    @property
    def chandelier_long_series(self):
        return pd.Series(self._long_list)

    @property
    def chandelier_short_series(self):
        return pd.Series(self._short_list)
    
    @property
    def atr(self):
        return self._atr_list[-1] if self._atr_list else None

    @property
    def chandelier_long(self):
        return self._long_list[-1] if self._long_list else None

    @property
    def chandelier_short(self):
        return self._short_list[-1] if self._short_list else None
