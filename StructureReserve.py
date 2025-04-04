from Structure import Structure
from utils import is_within_30_minutes_of_close
from datetime import timedelta

class StructureReserve(Structure):
    """
    该策略是Structure的反向策略
    Structure由于在近三个月的各类标的上亏损曲线都很稳定
    所以我决定把这个策略的买卖点信号都反过来
    
    首先对于开仓信号 直接反转direction就行
    其次对于平仓信号 还是要给到cal_exit_signal正向的direction(即反转direction取反)
    """
    def __init__(self, angle=0.02, dispear_angle=0.01, max_loss=0.01, max_profit=0.01, **kwargs):
        super().__init__(**kwargs)
        self.angle          = angle
        self.dispear_angle  = dispear_angle
        self.max_loss       = max_loss
        self.max_profit     = max_profit
        
    def cal(self, bars):
        df = self.prepare_data(bars)
        
        # 获取当前区块的 block_id
        current_block_id = df['block_id'].max()
        if current_block_id < 3: return False # 数据很短，没有信号
        
        current_block = self.get_block_by_id(current_block_id)
        previous_block = self.get_block_by_id(current_block_id - 2)
        result = self.compare_block(previous_block, current_block)
        if result is not None:
            return self.exclude_low_probability_structure(result, 0, current_block_id)
            
        if current_block_id > 4:
            earlier_block = self.get_block_by_id(current_block_id - 4)
            result = self.compare_block(earlier_block, current_block)
            if result is not None:
                return self.exclude_low_probability_structure(result, 1, current_block_id)

    def compare_block(self, block_1, block_2):
        """
            :param
            block_1: instanceof(pd.DataFrame) 要比较的对象 即前一个block
            block_2: instanceof(pd.DataFrame) 一般是当前block
        """
        # 顶背离时DIF值需要在zero axis上方
        if block_2['block_type'].sum() >= 1 and block_2['DIF'].sum() > 0: # 顶背离
            block_1_close   = block_1['close'].max()
            block_1_diff    = block_1['DIF'].max()
            block_2_close   = block_2['close'].max()
            block_2_diff    = block_2['DIF'].max()
            angle           = block_2.iloc[-1]['angle']
            # 股价新高，DIF不创新高，且最新的K线在调头
            if block_2_close > block_1_close and block_2_diff < block_1_diff and angle < -1 * self.angle:
                return "顶背离"
            
        if block_2['block_type'].sum() <= -1 and block_2['DIF'].sum() < 0: # 底背离
            block_1_close   = block_1['close'].min()
            block_1_diff    = block_1['DIF'].min()
            block_2_close   = block_2['close'].min()
            block_2_diff    = block_2['DIF'].min()
            angle           = block_2.iloc[-1]['angle']
            # 股价新低，DIF不创新低，且最新的K线在调头
            if block_2_close < block_1_close and block_2_diff > block_1_diff and angle > self.angle:
                return "底背离"
        
    def cal_exit_signal(self, bars, position_direction, entry_price, entry_time, holding_period=26):
        """
        :params
        bars: IBKR行情数据
        position_direction: 持仓数量,持仓数量大于0即多单,小于0是空单
        """
        assert position_direction != 0, "持仓数量不能为零，结合仓位管理运行"
        df = self.prepare_data(bars)
        
        current_price = df.iloc[-1]['close']
        time_elapsed = df.iloc[-1]['date'] - entry_time
        angle = df.iloc[-1]['angle']

        # 条件 1: MACD向背离方向变化
        if (position_direction < 0 and angle >= self.dispear_angle) or (position_direction > 0 and angle <= -1 * self.dispear_angle):
            return "平仓信号：MACD背离方向变化"

        # 条件 2: 亏损达到1%(其实在此处是盈利达到1%)
        price_change_pct = (current_price - entry_price) / entry_price
        if (position_direction > 0 and price_change_pct <= -1 * self.max_loss) or (position_direction < 0 and price_change_pct >= self.max_loss):
            return "平仓信号：达到亏损上限"
        
        # 条件 2: 盈利达到1%(其实在此处是亏损达到1%)
        if (position_direction > 0 and price_change_pct > self.max_profit) or (position_direction < 0 and price_change_pct < -1 * self.max_profit):
            return "平仓信号：达到盈利上限"
        
        # 条件 3: 持有时间超过26个周期
        if time_elapsed >= timedelta(minutes=holding_period):
            return "平仓信号：持有时间超限"

        return False  # 无平仓信号
    
    # def find_position(self, contract, pm):
    #     is_match = lambda item: (
    #         item["contract"] == contract and
    #         item["strategy"] == "Structure"
    #     )
    #     return pm.find_position(is_match)
    
    def find_position(self, contract, pm, bars):
        if pm.debug:
            # 获取当前时间的日期部分
            current_date = bars.iloc[-1]["date"].date()

            # 查找是否有开仓且日期为同一天的仓位
            is_match = lambda item: (
                item["contract"] == contract and
                item["strategy"] == "Structure" and
                item["date"].date() == current_date  # 确保是同一天
            )
            return pm.find_position(is_match)
        else:
            is_match = lambda item: (
                item["contract"] == contract and
                item["strategy"] == "Structure"
            )
            return pm.find_position(is_match)
        
    def find_trade(self, contract, pm, open_or_close):
        if pm.debug: return None
        else:
            is_match = lambda item: (
                item["trade"].contract == contract and
                item["strategy"] == "Structure" and
                item["open_or_close"] == open_or_close
            )
            return pm.find_trade(is_match)
        
    def update(self, contract, bars, pm):
        signal = self.cal(bars)
        position = self.find_position(contract, pm, bars)
        
        if not position:
            if signal and not is_within_30_minutes_of_close(bars):
                direction = -1 if signal == "底背离" else 1 # 反转direction
                amount = direction * pm.calculate_open_amount(bars)
                if amount != 0:
                    pm.open_position(contract, "Structure", amount, bars)
        else:
            # 平仓信号，因为持仓是反向的，所以此处对amount取反
            exit_signal = self.cal_exit_signal(bars, -1 * position["amount"], position["price"], position["date"])
            exit_amount = position["amount"] * -1 # 这是退出的数量及方向
            if exit_signal and not signal and not self.find_trade(contract, pm, "平仓"):
                pm.close_position(position, bars)
                
            # if exit_signal and signal:
            #     direction = -1 if signal == "底背离" else 1
            #     open_amount = direction * pm.calculate_open_amount(bars)
            #     if open_amount * exit_amount > 0:
            #         print(f"【{bars.iloc[-1]['date']}】【{contract.symbol}】反手")
            #         if not self.find_trade(contract, pm, "平仓"): pm.close_position(position, bars)
            #         if not self.find_trade(contract, pm, "开仓"): pm.open_position(contract, "Structure", open_amount, bars)