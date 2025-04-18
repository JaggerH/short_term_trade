import pandas as pd
import numpy as np
import pytz
from PlotPlus import PlotPlus

tz = pytz.timezone('US/Eastern')
class Chan:
    def __init__(self):
        self.df = pd.DataFrame()
        self.df_merged = pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]').dt.tz_localize(tz),
            'open': pd.Series([], dtype='float'),
            'high': pd.Series([], dtype='float'),
            'low': pd.Series([], dtype='float'),
            'close': pd.Series([], dtype='float'),
            'volume': pd.Series([], dtype='float'),
            'bi': pd.Series([], dtype='bool'),
            'pivot': pd.Series([], dtype='string')
        })
        self.has_find_bi = False
        self.effective_idxs = set()
        self.candle_candidate = None
        self.candle_direction = None
        self.candidate = 0
        self.direction = None
        self.valid = None
    
    def dynamic_merge(self, bar):
        """
        将新来的 bar 动态合并进已有的 df_merged，符合缠论的包含关系合并规则
        1. 设置了一个变量candle_candidate
        2. 直到有新的K线得区域超出了之前范围 将candle_candidate合并到df_merged中
        返回 True 表示存在新的K线
        """
        if self.candle_candidate is None:
            self.candle_candidate = bar
            return False

        # 检查是否存在包含关系
        A = self.candle_candidate
        B = bar

        is_contained = (
            (B['high'] <= A['high'] and B['low'] >= A['low']) or
            (A['high'] <= B['high'] and A['low'] >= B['low'])
        )
        # 没有包含关系会直接结束候选判定
        # candle_direction为空时数据不够，也无法进行合并处理，也结束候选判定
        if not is_contained or self.candle_direction is None:
            if B['high'] > A['high'] and B['low'] > A['low']:
                self.candle_direction = 'up'
            if B['high'] < A['high'] and B['low'] < A['low']:
                self.candle_direction = 'down'
            # 不存在包含关系 -> candidate 合并结束，加入 df_merged
            A['bi'] = False
            A['pivot'] = np.nan
            # if self.df_merged.empty:
            #     self.df_merged = pd.DataFrame([A], columns=['date', 'open', 'high', 'low', 'close', 'volume', 'bi', 'pivot'], dtype='object')
            # else:
            #     self.df_merged = pd.concat([self.df_merged, pd.DataFrame([A])], ignore_index=True)
            if self.df_merged.empty:
                self.df_merged = A.to_frame().T  # 把 Series 转成 1-row DataFrame
                self.df_merged['bi'] = self.df_merged['bi'].astype(bool)
                self.df_merged['pivot'] = self.df_merged['pivot'].astype('string')
            else:
                df = A.to_frame().T
                df['bi'] = df['bi'].astype(bool)
                df['pivot'] = df['pivot'].astype('string')
                # self.df_merged = pd.concat([self.df_merged, pd.DataFrame([A])], ignore_index=True)
                self.df_merged = pd.concat([self.df_merged, df], ignore_index=True)
            self.candle_candidate = B  # 新的 candidate
            return True

        # 根据方向合并 A 和 B，更新 candidate
        merged = A.copy() if A['high'] > B['high'] else B.copy()
        if self.candle_direction == 'up':
            merged['high'] = max(A['high'], B['high'])
            merged['low'] = max(A['low'], B['low'])
        else:
            merged['high'] = min(A['high'], B['high'])
            merged['low'] = min(A['low'], B['low'])

        merged['open'] = A['open']
        merged['close'] = B['close']
        self.candle_candidate = merged
        return False
    
    def dynamic_identify_fractals(self):
        """
        仅在 df_merged 的末尾判断是否构成新的顶/底分型，避免全量判断。
        如果构成分型，在对应的行添加 'top_fractal' 或 'bottom_fractal' 为 True。
        """
        if len(self.df_merged) < 3:
            return False # 不足3根，不能构成分型

        # 取最后三根
        sub = self.df_merged.iloc[-3:].copy()
        highs = sub['high'].values
        lows = sub['low'].values
        idx = sub.index[1]  # 中间K线的索引

        # 判断顶分型
        if highs[1] > highs[0] and highs[1] > highs[2] and lows[1] > lows[0] and lows[1] > lows[2]:
            self.df_merged.loc[idx, 'pivot'] = 'up'
            return True
        # 判断底分型
        elif lows[1] < lows[0] and lows[1] < lows[2] and highs[1] < highs[0] and highs[1] < highs[2]:
            self.df_merged.loc[idx, 'pivot'] = 'down'
            return True
        return False
        
    def update_bi(self, gap_threshold=4, log=False):
        # if len(self.df_merged) < 5: return
        df = self.df_merged
        df_pivot = df[df["pivot"].notnull()].reset_index().rename(columns={'index': 'orig_idx'})
        # if len(df_pivot) < 2: return
        i = df_pivot.index[-1]

        if not self.has_find_bi:
            # 提取 pivot 不为空的行，保留原始索引作为 orig_idx
            # 用于保存有效 pivot 对应的原始索引
            self.effective_idxs.add(self.candidate)
            self.has_find_bi = True
        else:
            start = df_pivot.loc[self.candidate, "orig_idx"]
            end = df_pivot.loc[i, "orig_idx"]
            last_pivot_type = df_pivot.loc[i, "pivot"]
            if df_pivot.loc[self.candidate, "pivot"] != last_pivot_type:
                if (end - start) >= gap_threshold:
                    if log:
                        range_value = None
                        if df.loc[start, "pivot"] == "up":
                            range_value = f'[{df.loc[end, "low"]}, {df.loc[start, "high"]}]'
                        if df.loc[start, "pivot"] == "down":
                            range_value = f'[{df.loc[start, "low"]}, {df.loc[end, "high"]}]'
                        print(f'Try to find Bi({last_pivot_type}):-------[{self.candidate}, {i}]---[{df.loc[start:end, "low"].min()}, {df.loc[start:end, "high"].max()}]--{range_value}')
                    if (df.loc[start, "pivot"] == "down" and df.loc[start, "low"] <= df.loc[start:end, "low"].min() and df.loc[end, "high"] >= df.loc[start:end, "high"].max()) or \
                        (df.loc[start, "pivot"] == "up" and df.loc[start, "high"] >= df.loc[start:end, "high"].max() and df.loc[end, "low"] <= df.loc[start:end, "low"].min()):
                        self.effective_idxs.add(self.candidate)
                        self.valid = self.candidate
                        self.candidate = i
                        if log: print(f'【CONFIRM】: Find Bi: valid_pivot: {self.valid}, candidate_pivot: {self.candidate}, self.effective_idxs: {self.effective_idxs}')
                        self.direction = 1 if last_pivot_type == "up" else -1
                        return [self.direction, 0]
                if self.valid and ((df_pivot.loc[self.valid, "pivot"] == "down" and df_pivot.loc[self.valid, "low"] > df_pivot.loc[i, "low"]) or \
                        (df_pivot.loc[self.valid, "pivot"] == "up" and df_pivot.loc[self.valid, "high"] < df_pivot.loc[i, "high"])):
                    # 纠错
                    while len(self.effective_idxs) > 1:
                        self.effective_idxs.remove(sorted(self.effective_idxs)[-1])
                        self.valid = sorted(self.effective_idxs)[-1]
                        if df_pivot.loc[self.valid, "pivot"] == "down":
                            if df_pivot.loc[self.valid, "low"] > df_pivot.loc[self.valid:i, "low"].min():
                                continue
                            else:
                                self.candidate = df_pivot.loc[self.valid:i, 'high'].idxmax()
                                break
                        if df_pivot.loc[self.valid, "pivot"] == "up":
                            if df_pivot.loc[self.valid, "high"] < df_pivot.loc[self.valid:i, "high"].max():
                                continue
                            else:
                                self.candidate = df_pivot.loc[self.valid:i, 'low'].idxmin()                                
                                break
                    if log: print(f'【纠错】current is {i}, 将valid重置为{self.valid}, candidate设为{self.candidate}')
                    self.direction = 1 if df_pivot.loc[self.valid, "pivot"] == "down" else -1
                    return [self.direction, 0]
            if df_pivot.loc[self.candidate, "pivot"] == df_pivot.loc[i, "pivot"]:
                if df_pivot.loc[i, "pivot"] == "up" and df_pivot.loc[self.candidate, "high"] < df_pivot.loc[i, "high"]:
                    if log: print(f'【Update candidate】: {self.valid}-{self.candidate}-{i}, type is up, price is from {df_pivot.loc[self.candidate, "high"]}-{df_pivot.loc[i, "high"]}')
                    self.candidate = i
                    return [self.direction, 1]
                elif df_pivot.loc[i, "pivot"] == "down" and df_pivot.loc[self.candidate, "low"] > df_pivot.loc[i, "low"]:
                    if log: print(f'【Update candidate】: {self.valid}-{self.candidate}-{i}, type is down, price is from {df_pivot.loc[self.candidate, "low"]}-{df_pivot.loc[i, "low"]}')
                    self.candidate = i
                    return [self.direction, 1]
                
    def update(self, bar, log=False):
        if self.dynamic_merge(bar):
            if self.dynamic_identify_fractals():
                result = self.update_bi(log=log)
                if result and result[1] == 0: # 确认笔
                    df = self.df_merged
                    df_pivot = df[df["pivot"].notnull()].reset_index().rename(columns={'index': 'orig_idx'})
                    orig_idx = df_pivot.loc[list(self.effective_idxs), "orig_idx"]
                    self.df_merged["bi"] = False
                    self.df_merged.loc[list(orig_idx), "bi"] = True
                    bi = self.df_merged[self.df_merged["bi"] & self.df_merged["pivot"].notna()]
                    # print(f'确认笔, current is {bar["date"]}, bi pivot is {bi.iloc[-1]["date"]}')
                return result
            
    def merge_pass(self, df):
        """
        对 df 进行一遍合并，返回 (merged_occurred, new_df)
        merged_occurred 为 True 表示这一遍有合并发生，否则为 False。
        """
        new_rows = []
        i = 0
        n = len(df)
        merged_occurred = False

        while i < n:
            if i == n - 1:
                # 最后一根K线直接加入
                new_rows.append(df.iloc[i])
                i += 1
            else:
                # 相邻两根K线
                row1 = df.iloc[i]
                row2 = df.iloc[i+1]
                # 判断包含关系：如果 row1 包含 row2 或 row2 包含 row1
                if (row1['high'] >= row2['high'] and row1['low'] <= row2['low']) or \
                (row2['high'] >= row1['high'] and row2['low'] <= row1['low']):
                    # 存在包含关系，进行合并
                    merged_occurred = True
                    # 新K线：open 取 row1 的 open，close 取 row2 的 close，
                    # high 为两者较大值，low 为两者较小值
                    new_bar = row1.copy() if row1['high'] >= row2['high'] else row2.copy()
                    new_bar['high'] = max(row1['high'], row2['high'])
                    new_bar['low'] = min(row1['low'], row2['low'])
                    new_bar['open'] = row1['open']
                    new_bar['close'] = row2['close']
                    new_rows.append(new_bar)
                    i += 2  # 合并后跳过下一根
                else:
                    # 不存在包含关系，直接保留 row1
                    new_rows.append(row1)
                    i += 1

        new_df = pd.DataFrame(new_rows)
        new_df.index = range(len(new_df))
        return merged_occurred, new_df

    def merge_candles(self, df):
        """
        递归合并包含关系的K线，直到再也不存在包含关系为止。
        返回合并后的 DataFrame。
        """
        df_merged = df.copy()
        while True:
            merged_occurred, df_new = self.merge_pass(df_merged)
            if not merged_occurred:
                break
            df_merged = df_new
        return df_merged
    
    def identify_fractals(self, df):
        """
        识别顶分型和底分型。
        返回两个布尔型 Series，分别表示顶分型（top_fractal）和底分型（bottom_fractal）。
        """
        # 顶分型：中间 K 线的最高价是相邻三根 K 线中最高的，且最低价也是相邻三根 K 线中最高的
        top_fractal = (
            (df['high'] > df['high'].shift(1)) &
            (df['high'] > df['high'].shift(-1)) &
            (df['low'] > df['low'].shift(1)) &
            (df['low'] > df['low'].shift(-1))
        )

        # 底分型：中间 K 线的最低价是相邻三根 K 线中最低的，且最高价也是相邻三根 K 线中最低的
        bottom_fractal = (
            (df['low'] < df['low'].shift(1)) &
            (df['low'] < df['low'].shift(-1)) &
            (df['high'] < df['high'].shift(1)) &
            (df['high'] < df['high'].shift(-1))
        )

        return top_fractal, bottom_fractal

    def add_pivot_flag(self, df):
        """
        在 df 中添加 pivot 列：
        对于顶分型，pivot 标记为 "up"；
        对于底分型，pivot 标记为 "down"。
        假设 df 已经包含布尔列 'top_fractal' 和 'bottom_fractal'
        """
        df = df.copy()
        # df['pivot'] = None
        # # 对loc[0]的位置做额外标记
        # row = df.iloc[0]
        # if row['open'] < row['close']:
        #     df.loc[0, 'top_fractal'] = True
        # if row['open'] > row['close']:
        #     df.loc[0, 'bottom_fractal'] = True
        
        df.loc[df['top_fractal'] == True, 'pivot'] = "up"
        df.loc[df['bottom_fractal'] == True, 'pivot'] = "down"
        return df

    def mark_effective_pivot(self, df, gap_threshold=4, log=False):
        """
        在 df 基础上添加一列 'bi'，对已标记的 pivot（非空）进行判断：
        如果相邻两个 pivot 之间的原始索引差值大于等于 gap_threshold，
        则认为这两个 pivot 是有效的（构成一个笔的边界），标记为 True，
        否则标记为 False。
        
        返回新的 DataFrame。
        """
        df = df.copy()
        # 默认 bi 列为 False
        df["bi"] = False
        
        # 提取 pivot 不为空的行，保留原始索引作为 orig_idx
        df_pivot = df[df["pivot"].notnull()].reset_index().rename(columns={'index': 'orig_idx'})
        # 用于保存有效 pivot 对应的原始索引
        effective_idxs = set()
        valid_pivot = None
        candidate_pivot = 0 # 给个默认值让程序能够正常允许
        effective_idxs.add(candidate_pivot)
        i = 1
        # 遍历相邻的 pivot 点
        while i < len(df_pivot):
            start = df_pivot.loc[candidate_pivot, "orig_idx"]
            end = df_pivot.loc[i, "orig_idx"]
            if df_pivot.loc[candidate_pivot, "pivot"] != df_pivot.loc[i, "pivot"]:
                if (end - start) >= gap_threshold:
                    if log:
                        direction = df_pivot.loc[i, "pivot"]
                        range_value = None
                        if df.loc[start, "pivot"] == "up":
                            range_value = f'[{df.loc[end, "low"]}, {df.loc[start, "high"]}]'
                        if df.loc[start, "pivot"] == "down":
                            range_value = f'[{df.loc[start, "low"]}, {df.loc[end, "high"]}]'
                        print(f'Try to find Bi({direction}):-------[{candidate_pivot}, {i}]---[{df.loc[start:end, "low"].min()}, {df.loc[start:end, "high"].max()}]--{range_value}')
                    if (df.loc[start, "pivot"] == "down" and df.loc[start, "low"] <= df.loc[start:end, "low"].min() and df.loc[end, "high"] >= df.loc[start:end, "high"].max()) or \
                        (df.loc[start, "pivot"] == "up" and df.loc[start, "high"] >= df.loc[start:end, "high"].max() and df.loc[end, "low"] <= df.loc[start:end, "low"].min()):
                        effective_idxs.add(candidate_pivot)
                        valid_pivot = candidate_pivot
                        candidate_pivot = i
                        if log: print(f'【CONFIRM】: Find Bi: valid_pivot: {valid_pivot}, candidate_pivot: {candidate_pivot}, effective_idxs: {effective_idxs}')
                        i += 1
                        continue
                if valid_pivot and ((df_pivot.loc[valid_pivot, "pivot"] == "down" and df_pivot.loc[valid_pivot, "low"] > df_pivot.loc[i, "low"]) or \
                        (df_pivot.loc[valid_pivot, "pivot"] == "up" and df_pivot.loc[valid_pivot, "high"] < df_pivot.loc[i, "high"])):
                    # 纠错
                    while len(effective_idxs) > 1:
                        effective_idxs.remove(sorted(effective_idxs)[-1])
                        valid_pivot = sorted(effective_idxs)[-1]
                        if df_pivot.loc[valid_pivot, "pivot"] == "down":
                            if df_pivot.loc[valid_pivot, "low"] > df_pivot.loc[valid_pivot:i, "low"].min():
                                continue
                            else:
                                candidate_pivot = df_pivot.loc[valid_pivot:i, 'high'].idxmax()
                                break
                        if df_pivot.loc[valid_pivot, "pivot"] == "up":
                            if df_pivot.loc[valid_pivot, "high"] < df_pivot.loc[valid_pivot:i, "high"].max():
                                continue
                            else:
                                candidate_pivot = df_pivot.loc[valid_pivot:i, 'low'].idxmin()                                
                                break
                    if log: print(f'【纠错】current is {i}, 将valid重置为{valid_pivot}, candidate设为{candidate_pivot}')
                    i += 1
                    continue
            if df_pivot.loc[candidate_pivot, "pivot"] == df_pivot.loc[i, "pivot"]:
                if df_pivot.loc[i, "pivot"] == "up" and df_pivot.loc[candidate_pivot, "high"] < df_pivot.loc[i, "high"]:
                    if log: print(f'【Update candidate】: {valid_pivot}-{candidate_pivot}-{i}, type is up, price is from {df_pivot.loc[candidate_pivot, "high"]}-{df_pivot.loc[i, "high"]}')
                    candidate_pivot = i
                elif df_pivot.loc[i, "pivot"] == "down" and df_pivot.loc[candidate_pivot, "low"] > df_pivot.loc[i, "low"]:
                    if log: print(f'【Update candidate】: {valid_pivot}-{candidate_pivot}--{i}, type is down, price is from {df_pivot.loc[candidate_pivot, "low"]}-{df_pivot.loc[i, "low"]}')
                    candidate_pivot = i
                # else:
                    # if log: print(f'IGNORE: current is {i}, type is {df_pivot.loc[i, "pivot"]}, candidate is {candidate_pivot}, type is {df_pivot.loc[candidate_pivot, "pivot"]}')
            i += 1
        # 将有效 pivot 对应行的 bi 标记为 True
        effective_idxs.remove(0)
        orig_idx = df_pivot.loc[list(effective_idxs), "orig_idx"]
        df.loc[list(orig_idx), "bi"] = True
        return df

    def process(self, bars, log=False):
        bars = bars.copy()
        for index, bar in bars.iterrows():
            self.dynamic_merge(bar)
        df = self.df_merged.copy()
        self.df_merged = pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]').dt.tz_localize(tz),
            'open': pd.Series([], dtype='float'),
            'high': pd.Series([], dtype='float'),
            'low': pd.Series([], dtype='float'),
            'close': pd.Series([], dtype='float'),
            'volume': pd.Series([], dtype='float'),
            'bi': pd.Series([], dtype='bool'),
            'pivot': pd.Series([], dtype='string')
        })
        # df = self.merge_candles(bars)
        # print(df)
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype('float')
        df['high'] = df['high'].astype('float')
        df['low'] = df['low'].astype('float')
        df['close'] = df['close'].astype('float')
        df['volume'] = df['volume'].astype('int')
        
        top, bottom = self.identify_fractals(df)
        df['top_fractal'] = top
        df['bottom_fractal'] = bottom

        df = self.add_pivot_flag(df) # 将顶底分型标记在pivot中，以up\down显示，默认为None
        df = self.mark_effective_pivot(df, log=log)
        
        bars = pd.merge(bars, df[['date', 'pivot', 'bi']], on='date', how='left')
        bars['bi'] = bars['bi'].astype('bool')
        bars['bi'] = bars['bi'].fillna(False)
        return bars, df
    
    def debug(self, daily, ba, idx, debug=True):
        date = daily.iloc[idx]['date']
        print(idx, date)
        idx += 1
        df = ba.get_historical_data(ba.contracts[0], date)
        df, df_merged = self.process(df, log=debug)

        pp = PlotPlus(df if not debug else df_merged)
        pp.plot_basic(style_type="candle")
        # mark pivot
        if not debug:
            pp.mark_point(df[df["bi"] & (df['pivot'] == 'up')], "high")
            pp.mark_point(df[df["bi"] & (df['pivot'] == 'down')], "low")
        
        # debug
        if debug:
            pp.mark_point(df[df['pivot'] == 'up'], "high", color='red')
            pp.mark_point(df[df['pivot'] == 'down'], "low", color='green')
            pp.mark_point(df[df["bi"] & (df['pivot'] == 'up')], "high")
            pp.mark_point(df[df["bi"] & (df['pivot'] == 'down')], "low")
        pp.show()
        return df, df_merged
    
    def find_segments(self, df=None):
        """
        从已标注 pivot（'up'/'down'）和 bi（True/False）的 DataFrame 中，
        识别缠论线段，包含“潜在终结 + 下一笔破位”规则。
        """
        df = df if df is not None else self.df_merged
        pivots = df.loc[df["pivot"].notna() & df["bi"], ["pivot", "high", "low", "date"]].copy()
        pivots["price"] = pivots.apply(
            lambda r: r["high"] if r["pivot"] == "up" else r["low"], axis=1
        )

        segments = []
        if len(pivots) < 3:
            return segments

        it = pivots.itertuples()
        first = next(it)
        start_idx, start_price = first.Index, first.price
        last_idx, last_price = start_idx, start_price
        direction = None
        fail_flag = False
        fail_idx = None
        fail_price = None

        for row in it:
            print(row)
            idx, pivot, high, low, date, price = row.Index, row.pivot, row.high, row.low, row.date, row.price

            if direction is None:
                direction = "up" if price > start_price else "down"
                last_idx, last_price = idx, price
                print(f"First segment: {df.loc[start_idx, 'date']} - {df.loc[last_idx, 'date']}, price: {start_price} - {last_price}, direction: {direction}")
                continue

            if direction == "up":
                if price > last_price:
                    # 正常创新高，延续
                    last_idx, last_price = idx, price
                    fail_flag = False
                else:
                    if not fail_flag and price <= start_price:
                        # 标记潜在终结点
                        fail_flag = True
                        fail_idx, fail_price = last_idx, last_price
                    elif fail_flag and price < fail_price:
                        # 确认破位，划分线段
                        segments.append((
                            df.at[start_idx, "date"],
                            df.at[fail_idx, "date"]
                        ))
                        # 启动新一段
                        start_idx, start_price = fail_idx, fail_price
                        direction = "down"
                        last_idx, last_price = idx, price
                        fail_flag = False
                print(f"Up segment: {df.loc[start_idx, 'date']} - {df.loc[last_idx, 'date']}, price: {start_price} - {last_price}, direction: {direction}")
            else:  # direction == "down"
                if price < last_price:
                    last_idx, last_price = idx, price
                    fail_flag = False
                else:
                    if not fail_flag and price >= start_price:
                        fail_flag = True
                        fail_idx, fail_price = last_idx, last_price
                    elif fail_flag and price > fail_price:
                        segments.append((
                            df.at[start_idx, "date"],
                            df.at[fail_idx, "date"]
                        ))
                        start_idx, start_price = fail_idx, fail_price
                        direction = "up"
                        last_idx, last_price = idx, price
                        fail_flag = False
                print(f"Down segment: {df.loc[start_idx, 'date']} - {df.loc[last_idx, 'date']}, price: {start_price} - {last_price}, direction: {direction}")

        # 修正收尾逻辑：如果最后一笔和start不同，补上最后一段
        if last_idx != start_idx:
            segments.append((
                df.at[start_idx, "date"],
                df.at[last_idx, "date"]
            ))

        return segments
