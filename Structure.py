import talib
import pandas as pd
from datetime import timedelta
from utils import macd
from sklearn.preprocessing import QuantileTransformer

ANGLE = 0.015
SCALED_BANDS = 0.07
DISPEAR_ANGLE = 0.02

class Structure:
    def __init__(self):
        self.has_prepare_data = False
        self.data = None
        
    def prepare_data(self, bars):
        if self.has_prepare_data: return self.data
        if not isinstance(bars, pd.DataFrame):
            df = pd.DataFrame(bars)
        else:
            df = bars
        
        df['DIF'], df['DEA'], df['MACD'] = macd(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df = process_blocks(df)
        self.data = df
        self.has_prepare_data = True
        return df
    
    def get_block_by_id(self, id):
        return self.data[self.data['block_id'] == id]
    
    def cal(self, bars):
        if not self.has_prepare_data: self.prepare_data(bars)
        df = self.data
        
        # 获取当前区块的 block_id
        current_block_id = df['block_id'].max()
        if current_block_id < 3: return False # 数据很短，没有信号
        
        current_block = self.get_block_by_id(current_block_id)
        previous_block = self.get_block_by_id(current_block_id - 2)
        result = compare_block(previous_block, current_block)
        if result is not None:
            return self.exclude_low_probability_structure(result, 0, current_block_id)
            
        if current_block_id > 4:
            earlier_block = self.get_block_by_id(current_block_id - 4)
            result = compare_block(earlier_block, current_block)
            if result is not None:
                return self.exclude_low_probability_structure(result, 1, current_block_id)
                
    def exclude_low_probability_structure(self, result, gap_type, current_block_id):
        """
        如果是低概率的结构 会直接返回None
        Args:
            result (str): 不参与运算
            gap_type (int): 0-临峰 1-隔峰
            current_block_id (int): 当前区块id

        Returns:
            None or result
        """
        df = self.data
        if gap_type == 0:
            middle_block = self.get_block_by_id(current_block_id - 1)
            related_blocks = df[(df['block_id'] >= current_block_id - 2) & (df['block_id'] <= current_block_id)]
            if block_not_cross_zero_axis(middle_block, result) and not trend_convergence(related_blocks):
                return result
        
        if gap_type == 1:
            middle_block_1 = self.get_block_by_id(current_block_id - 1)
            middle_block_2 = self.get_block_by_id(current_block_id - 3)
            related_blocks = df[(df['block_id'] >= current_block_id - 4) & (df['block_id'] <= current_block_id)]
            if block_not_cross_zero_axis(middle_block_1, result) and block_not_cross_zero_axis(middle_block_2, result) and not trend_convergence(related_blocks):
                return result
        
    def cal_exit_signal(self, bars, position_direction, entry_price, entry_time, max_loss=0.01, holding_period=26):
        """
        :params
        bars: IBKR行情数据
        position_direction: 直接给持仓数量也行,持仓数量大于0即多单,小于0是空单
        """
        assert position_direction != 0, "持仓数量不能为零，结合仓位管理运行"
        if not self.has_prepare_data: self.prepare_data(bars)
        df = self.data
        
        current_price = df.iloc[-1]['close']
        time_elapsed = df.iloc[-1]['date'] - entry_time
        angle = df.iloc[-1]['angle']

        # 条件 1: MACD向背离方向变化
        if (position_direction < 0 and angle >= DISPEAR_ANGLE) or (position_direction > 0 and angle <= -1 * DISPEAR_ANGLE):
            return "平仓信号：MACD背离方向变化"

        # 条件 2: 亏损达到1%
        if (entry_price - current_price) / entry_price >= max_loss:
            return "平仓信号：达到亏损上限"

        # 条件 3: 持有时间超过26个周期
        if time_elapsed >= timedelta(minutes=holding_period):
            return "平仓信号：持有时间超限"

        return False  # 无平仓信号
    
def get_prev_blockID(df, block_id):
    """
    为什么使用函数获取PrevBlockID？
    因为MACD存在连续的单柱在红绿柱之间变化
    这会导致获取的PrevBlock实际是empty的导致报错
    将这种单个柱子均并入上一个柱子
    """
    prev_block_id = block_id
    while True:
        if block_id == 1: return 1
        prev_block_id = prev_block_id - 1
        prev_block = df[df['block_id'] == prev_block_id]
        if not prev_block.empty:
            return prev_block_id
        
def process_blocks(df):
    """
    根据df['macd']生成连续的block并合并单柱block。

    Args:
        df (pd.DataFrame): 包含`macd`列的DataFrame。

    Returns:
        pd.DataFrame: 增加了`block_type`和`block_id`列的DataFrame。
    """
    # 初始化 block_type
    df['block_type'] = df['MACD'].apply(lambda x: 1 if x >= 0 else -1)
    
    # 初始化 block_id
    df['block_id'] = (df['block_type'] != df['block_type'].shift()).cumsum()

    # 查找需要合并的单柱 block
    block_sizes = df.groupby('block_id')['block_type'].size()
    single_blocks = block_sizes[block_sizes == 1].index  # 单柱的 block_id

    # 合并单柱 block
    for block_id in single_blocks:
        # 获取前后的 block_id 和类型
        prev_block_id = get_prev_blockID(df, block_id)
        if prev_block_id in block_sizes.index:
            df.loc[df['block_id'] == block_id, 'block_id'] = prev_block_id

    # 重新编号 block_id，确保连续
    df['block_id'] = df.groupby('block_id').ngroup() + 1
    
    # 对DIF归一化处理，缩放到[-1,1]之间，便于计算angle
    n_quantiles = min(len(df), 1000)  # 设置最大值为1000，避免过大的量
    scaler = QuantileTransformer(output_distribution='uniform', n_quantiles=n_quantiles)
    quantile_scaled = scaler.fit_transform(df[["DIF"]])
    df["DIF_scaled"] = 2 * quantile_scaled - 1
    df["angle"] = df["DIF_scaled"].diff()

    quantile_scaled = scaler.fit_transform(df[["DEA"]])
    df["DEA_scaled"] = 2 * quantile_scaled - 1
    
    return df

def compare_block(block_1, block_2):
    """
        :param
        block_1: instanceof(pd.DataFrame) 要比较的对象 即前一个block
        block_2: instanceof(pd.DataFrame) 一般是当前block
    """
    if block_2['block_type'].sum() >= 1 and block_2['DIF'].sum() > 0: # 顶背离
        block_1_close   = block_1['close'].max()
        block_1_diff    = block_1['DIF'].max()
        block_2_close   = block_2['close'].max()
        block_2_diff    = block_2['DIF'].max()
        angle           = block_2.iloc[-1]['angle']
        # 股价新高，DIF不创新高，且最新的K线在调头
        if block_2_close > block_1_close and block_2_diff < block_1_diff and angle < -1 * ANGLE:
            return "顶背离"
        
    if block_2['block_type'].sum() <= -1 and block_2['DIF'].sum() < 0: # 底背离
        block_1_close   = block_1['close'].min()
        block_1_diff    = block_1['DIF'].min()
        block_2_close   = block_2['close'].min()
        block_2_diff    = block_2['DIF'].min()
        angle           = block_2.iloc[-1]['angle']
        # 股价新低，DIF不创新低，且最新的K线在调头
        if block_2_close < block_1_close and block_2_diff > block_1_diff and angle > ANGLE:
            return "底背离"

def block_not_cross_zero_axis(block, structure_type):
    if structure_type == "顶背离" and (block["DIF"] < 0).any():
        return False
    
    if structure_type == "底背离" and (block["DIF"] > 0).any():
        return False
        
    return True
    
def trend_convergence(df, threshold=0.6):
    """
    过滤掉DIF和DEA线走势粘合的信号

    参数:
    df: 包含DIF和DEA列的DataFrame
    window: 用于计算粘合的窗口大小（例如50行）
    threshold: 重合比例的阈值，若超过该比例则认为信号无效

    返回:
    返回一个经过滤的DataFrame
    """
    # 获取当前窗口内的DIF和DEA值
    dif_vals = df['DIF_scaled']
    dea_vals = df['DEA_scaled']
    
    # 计算每行DIF和DEA的区间是否重合
    dif_upper = dif_vals + SCALED_BANDS
    dif_lower = dif_vals - SCALED_BANDS
    dea_upper = dea_vals + SCALED_BANDS
    dea_lower = dea_vals - SCALED_BANDS
    
    # 判断是否重合：检查区间是否交集
    is_coveraged = (dif_lower <= dea_upper) & (dif_upper >= dea_lower)
    
    # print(df.iloc[0]["date"], df.iloc[-1]["date"], is_coveraged.sum() / len(df) >= threshold)
    # 如果重合超过阈值，则标记为True
    return is_coveraged.sum() / len(df) >= threshold
    