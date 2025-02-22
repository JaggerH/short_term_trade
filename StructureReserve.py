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
    def __init__(self, dispear_angle=0.04, max_loss=0.02, max_profit=0.04, **kwargs):
        super().__init__(**kwargs)
        self.dispear_angle  = dispear_angle
        self.max_loss       = max_loss
        self.max_profit     = max_profit
        
    def cal_exit_signal(self, bars, position_direction, entry_price, entry_time, holding_period=26):
        """
        :params
        bars: IBKR行情数据
        position_direction: 持仓数量,持仓数量大于0即多单,小于0是空单
        """
        assert position_direction != 0, "持仓数量不能为零，结合仓位管理运行"
        if not self.has_prepare_data: self.prepare_data(bars)
        df = self.data
        
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
        
    def update(self, contract, bars, pm):
        signal = self.cal(bars)
        position = self.find_position(contract, pm, bars)
        if not position:
            if signal and not is_within_30_minutes_of_close(bars):
                direction = -1 if signal == "底背离" else 1 # 反转direction
                amount = direction * pm.calculate_open_amount(bars)
                pm.open_position(contract, "Structure", amount, bars)
        else:
            # 平仓信号，因为持仓是反向的，所以此处对amount取反
            exit_signal = self.cal_exit_signal(bars, -1 * position["amount"], position["price"], position["date"])
            exit_amount = position["amount"] * -1 # 这是退出的数量及方向
            if exit_signal and not signal:
                pm.close_position(position, bars)
                
            if exit_signal and signal:
                direction = -1 if signal == "底背离" else 1
                open_amount = direction * pm.calculate_open_amount(bars)
                if open_amount * exit_amount > 0:
                    print(f"【{bars.iloc[-1]['date']}】【{contract.symbol}】反手")
                    pm.close_position(position, bars)
                    pm.open_position(contract, "Structure", open_amount, bars)