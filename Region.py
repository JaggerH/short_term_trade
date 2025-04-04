import pandas as pd
import numpy as np

def compute_slope(prices):
    # 例如使用最简单的线性拟合，这里假设 prices 为 numpy 数组
    x = np.arange(len(prices))
    # 使用最小二乘法计算斜率：slope = Cov(x, y) / Var(x)
    slope = np.cov(x, prices)[0, 1] / np.var(x)
    return slope

def find_candidate_regions(df_trim, direction, noise_threshold, min_window, log=False):
    """
    从 df_trim 中，根据给定的噪音阈值寻找候选区间，
    并对候选区间的尾部进行修剪（去除因兼容噪音阈值而加入的尾部噪音K线）。
    返回候选区间列表，每个候选区间包含其起始和结束索引。
    假设 df_trim 的列名均为小写，例如 'high' 和 'low'。
    """
    candidates = []
    df = df_trim.copy()
    if direction == 1:
        df['extreme'] = df['low'].rolling(window=noise_threshold + 1).min()
        # 这里 raw=False 保证传入的 x 是 Series，从而能调用 idxmin()
        df['extreme_idx'] = df['low'].rolling(window=noise_threshold + 1, min_periods=noise_threshold)\
            .apply(lambda x: x.idxmin(), raw=False)
    else:
        df['extreme'] = df['high'].rolling(window=noise_threshold + 1).max()
        # 这里 raw=False 保证传入的 x 是 Series，从而能调用 idxmax()
        df['extreme_idx'] = df['high'].rolling(window=noise_threshold + 1, min_periods=noise_threshold)\
            .apply(lambda x: x.idxmax(), raw=False)
            
    n = df.iloc[-1].name
    i_start = df.iloc[0].name
    i = i_start
    touch_end = False
    while i < n:
        noise_count = 1
        start = i
        j = i + 1
        prev_extreme = df.loc[i, 'high'] if direction == 1 else df.loc[i, 'low']
        # 扩展候选区间，直到噪音次数超过阈值
        while j < n and noise_count <= noise_threshold:
            if direction == 1:
                # 上涨：如果当前 high 小于前一根 high，则计入噪音
                if df.loc[j, 'high'] <= prev_extreme:
                    noise_count += 1
                else:
                    prev_extreme = df.loc[j, 'high']
                    noise_count = 1
            else:
                # 下跌：如果当前 low 大于前一根 low，则计入噪音
                if df.loc[j, 'low'] >= prev_extreme:
                    noise_count += 1
                else:
                    prev_extreme = df.loc[j, 'low']
                    noise_count = 1
            j += 1
            if j >= n: touch_end = True
        # 如果候选区间覆盖了整个 df，则不计入候选结果
        if log: print(f"{j}, start: {i}, end: {j}")
        if not (start == i_start and j-1 == n-1):
            # 对候选区间进行尾部修剪
            if direction == 1:
                trimmed_end = df.loc[start:j, 'high'].idxmax()
                trimmed_start = df.loc[start:trimmed_end, 'low'].idxmin() # 修复：对于V型反转在end后面出现低点，大于在开头低点，导致错误排除合格的区间
            else:
                trimmed_end = df.loc[start:j, 'low'].idxmin()
                trimmed_start = df.loc[start:trimmed_end, 'high'].idxmax() # 修复：对于V型反转在end后面出现低点，大于在开头低点，导致错误排除合格的区间
            
            if trimmed_end - trimmed_start >= min_window:
                candidates.append((trimmed_start, trimmed_end))
                if log: print(f"{j} - trimmed_start: {trimmed_start}, trimmed_end: {trimmed_end}")
            else:
                if log: print(f"{j} - not quantified trimmed_start: {trimmed_start}, trimmed_end: {trimmed_end}")
        if touch_end or (j == df.iloc[-1].name): break
        i = df.loc[j, 'extreme_idx'] # 从 j 处极值idx继续寻找下一个候选区间
        
    return candidates

def search_candidates_with_increasing_noise(df_trim, direction, overall_slope, max_noise=5, min_window=5):
    # 从噪音阈值从 0 开始，递增寻找候选区间，直到满足条件（例如候选区间数为 1）或达到上限
    noise_threshold = 5
    final_candidates = []
    while noise_threshold <= max_noise:
        candidates = find_candidate_regions(df_trim, direction, noise_threshold, min_window)
        qualified = []
        # 对每个候选区间计算斜率，符合条件的候选区间保留
        for start, end in candidates:
            if direction == 1:
                prices = df_trim.loc[start:end, 'high'].values
            else:
                prices = df_trim.loc[start:end, 'low'].values
            candidate_slope = compute_slope(prices)
            # 如果候选区间斜率绝对值大于整体斜率（这里整体斜率取绝对值比较），则视为有效候选
            if (direction == 1 and candidate_slope > overall_slope) or (direction == -1 and candidate_slope < overall_slope):
                qualified.append({
                    'start': start,
                    'end': end,
                    'slope': candidate_slope,
                    'length': end - start + 1
                })
        
        # 当候选区间数量为 1 或者噪音阈值已经达到上限时退出
        if len(qualified) <= 1 or noise_threshold == max_noise:
            final_candidates = qualified
            break
        noise_threshold += 1
    return final_candidates, noise_threshold

def score_candidates(df_trim, candidates, direction, alpha=1.0, beta=1.0, gamma=1.0, delta=1.0):
    n = df_trim.iloc[-1].name
    scored = []
    for candidate in candidates:
        start, end = candidate['start'], candidate['end']
        length = candidate['length']
        # 时间因子：候选区间末端距离 df_trim 末尾的行数差距
        time_distance = (n - 1) - end
        # 振幅：上升时用 High 变化，下跌时用 Low 变化（确保为正值）
        if direction == 1:
            amplitude = df_trim.loc[end, 'high'] - df_trim.loc[start, 'high']
        else:
            amplitude = df_trim.loc[start, 'low'] - df_trim.loc[end, 'low']
        
        slope_factor = abs(candidate['slope'])
        # 组合得分：各指标间权重可根据实际调整
        score = alpha * slope_factor + beta * amplitude - gamma * length - delta * time_distance
        candidate['amplitude'] = amplitude
        candidate['time_distance'] = time_distance
        candidate['score'] = score
        scored.append(candidate)
    return scored

def trim_df(df, window):
    if len(df) > window:
        return df.iloc[-window:]
    return df

def find_pulse_regions(df, direction, window, max_noise=5, min_window=4, alpha=1.0, beta=1.0, gamma=1.0, delta=1.0):
    """
    Arguments:
    min_window: 一个region的最小长度
    """
    # 1. 截取最新行情
    df_trim = trim_df(df, window)
    # 2. 计算整体斜率
    if direction == 1:
        overall_prices = df_trim['high'].values
    else:
        overall_prices = df_trim['low'].values
    overall_slope = compute_slope(overall_prices)
    # 3. 噪音递增查找候选区间
    candidates, used_noise = search_candidates_with_increasing_noise(df_trim, direction, overall_slope, max_noise, min_window)
    # 4. 对候选区间评分
    scored_candidates = score_candidates(df_trim, candidates, direction, alpha, beta, gamma, delta)
    
    return df_trim, scored_candidates, used_noise

def mark_region(df, window=90):
    df['region'] = 'sideway'

    df_trim, up_candidates, use_noise = find_pulse_regions(df, direction=1, window=window, min_window=3)
    df_trim, down_candidates, use_noise = find_pulse_regions(df, direction=-1, window=window, min_window=3)

    for start, end in up_candidates:
        df.loc[start:end, 'region'] = 'up'

    for start, end in down_candidates:
        df.loc[start:end, 'region'] = 'down'
        
    return df