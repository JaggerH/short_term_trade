from utils import vwap

MONOTONIC_THRESHOLD = 0.01 # 判断价格趋势单调运行的阈值
DRAWDOWN_PERCENT    = 0.5 # 判断趋势的最大回撤幅度

class Trend:
    """
    日内趋势做法，在于找到日内单边走势
    Args:
    direction(int): 这个是用于开盘决策
    """
    def __init__(self, direction):
        self.direction = direction
    
    def cal(self, df):
        """
        计算close和vwap的差值并累加
        得到的结果area_sum取绝对值超过阈值MONOTONIC_THRESHOLD * len(df)判断价格在单边运行
        """
        if "vwap" not in df.columns:
            df["vwap"] = vwap(df["close"], df["volume"])
            
        df['cross_vwap'] = 0  # 初始化为 0
        
        df.loc[(df['close'] > df['vwap']) & (df['close'].shift(1) <= df['vwap'].shift(1)), 'cross_vwap'] = 1
        df.loc[(df['close'] < df['vwap']) & (df['close'].shift(1) >= df['vwap'].shift(1)), 'cross_vwap'] = -1
        
        df['up_or_down'] = df.apply(lambda row: 1 if row['close'] > row['vwap'] else (-1 if row['close'] < row['vwap'] else 0), axis=1)
        df['area'] = (df['close'] - df['vwap']) / df['vwap']
        
        # area_sum大于一定的阈值，可以判断股价在单调运行
        area_sum = df['area'].sum()
        if abs(area_sum) > MONOTONIC_THRESHOLD * len(df):
            # 此处判定，趋势为单调上升或下降
            # return True
            amplitude = calculate_amplitude(df)
            if area_sum > 0:
                max_drawdown = calculate_max_drawdown(df)
                if max_drawdown / amplitude < DRAWDOWN_PERCENT:
                    print(df.iloc[-1]["date"], "max_drawdown", max_drawdown / amplitude)
                    return "单边上涨"
            if area_sum < 0:
                max_rally = calculate_max_rally(df)
                if max_rally / amplitude < DRAWDOWN_PERCENT:
                    print(df.iloc[-1]["date"], "max_rally", max_rally / amplitude)
                    return "单边下跌"

def calculate_amplitude(df):
    """
    振幅
    """
    max_price = df['close'].max()
    min_price = df['close'].min()
    return (max_price - min_price) / min_price
    
def calculate_max_drawdown(df):
    """
    最大回撤幅度
    """
    # 计算累计最大值
    cumulative_max = df['close'].cummax()
    # 计算回撤（当前值与历史最大值的比率）
    drawdowns = (df['close'] - cumulative_max) / cumulative_max
    # 返回最大回撤值
    max_drawdown = drawdowns.min()
    return max_drawdown

def calculate_max_rally(df):
    """
    最大反弹幅度
    """
    # 计算累计最小值
    cumulative_min = df['close'].cummin()
    # 计算反弹（当前值与历史最小值的比率）
    rallies = (df['close'] - cumulative_min) / cumulative_min
    # 返回最大反弹幅度
    max_rally = rallies.max()
    return max_rally