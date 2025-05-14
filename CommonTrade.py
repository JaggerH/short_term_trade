from ChandelierExit import ChandelierExit
from PositionManagerPlus import PositionManager
from utils import is_within_specific_minutes_of_close

class CommonTradeConfig:
    def __init__(self, config:dict = {}):
        self.config = config
        self.config.setdefault("open_pct", 0.1)
        self.config.setdefault("open_before", 30)  # 默认收盘前30分钟允许开仓, -1不做限制
        self.config.setdefault("close_before", 2)  # 默认收盘前1分钟平仓，-1不做限制
        self.config.setdefault("chandelier_exit", False) # 默认关闭吊灯止盈
        
    def get_config(self, key, default=None):
        return self.config.get(key, default)
    
class CommonTrade:
    def __init__(self, contract, pm: PositionManager, config:dict = {}):
        self.contract = contract
        self.pm = pm
        self.config = CommonTradeConfig(config)
        self.bars = None
        self.cdlr = ChandelierExit()
        self.cldr_last_update = None
    
    def has_position(self):
        """
        是否有持仓
        :return: bool
        """
        position = self.find_position()
        return position is not None and position["amount"] != 0
    
    def find_position(self):
        is_match = lambda item: (
            item["contract"] == self.contract and
            item["strategy"] == self.__class__.__name__
        )
        return self.pm.find_position(is_match)
    
    def time_allow_open(self):
        """
        当前时间是否允许开仓
        通过配置open_before判断
        :return: bool
        """
        if self.config.get_config("open_before") == -1:
            return True
        else:
            return not is_within_specific_minutes_of_close(self.bars, self.config.get_config("open_before"))
        
    def cal_open_amount_by_pct(self):
        """
        计算开仓数量
        :param pct: 占用资金比例
        :return: 开仓数量
        """
        if self.pm.net_liquidation is None or self.pm.available_funds is None:
            print("PositionManager.calculate_open_amount net_liquidation or available_funds is None")
            return 0
        
        target_market_value = self.pm.net_liquidation * self.config.get_config("open_pct")
        if target_market_value > self.pm.available_funds: return 0
        
        open_amount = target_market_value / self.bars.iloc[-1]['close']
        open_amount = round(open_amount / 10) * 10  # 调整为 10 的倍数
        return int(open_amount)
    
    def chandelier_allow_open(self, direction):
        if not self.config.get_config("chandelier_exit"): return True
        if direction > 0 and self.cdlr.chandelier_long and self.bars.iloc[-1]["close"] > self.cdlr.chandelier_long:
            return True
        if direction < 0 and self.cdlr.chandelier_short and self.bars.iloc[-1]["close"] < self.cdlr.chandelier_short:
            return True
        return False
    
    def open_position(self, direction, reason=None):
        """
        开仓
        :param direction: 开仓方向
        :return: None
        """
        position = self.find_position()
        # 反手前 平仓
        if position and position["amount"] * direction < 0:
            self.pm.close_position(position, self.bars, reason="反手平仓")
        
        position = self.find_position()
        amount = direction * self.cal_open_amount_by_pct()
        if not position and self.time_allow_open() and self.chandelier_allow_open(direction):
            self.pm.open_position(self.contract, self.__class__.__name__, amount, self.bars, reason=reason)

    def close_position(self, reason=None):
        position = self.find_position()
        if position:
            self.pm.close_position(position, self.bars, reason=reason)
        
    def close_position_before_market_close(self):
        """
        是否在收盘前平仓
        """
        if self.config.get_config("close_before") == -1:
            return False
        elif is_within_specific_minutes_of_close(self.bars, self.config.get_config("close_before")):
            self.close_position("收盘前平仓")
    
    def update_cdlr(self, bars):
        if self.cldr_last_update is None or self.cldr_last_update < bars.iloc[-1]["date"]:
            self.cdlr.update(bars.iloc[-1])
            self.cldr_last_update = bars.iloc[-1]["date"]
            
    def close_position_by_chandier_exit(self, bars):
        if not self.config.get_config('chandelier_exit'): return
        position = self.find_position()
        if position and position["amount"] > 0 and self.cdlr.chandelier_long and bars.iloc[-1]["close"] < self.cdlr.chandelier_long:
            self.close_position("触发吊灯止盈")
        if position and position["amount"] < 0 and self.cdlr.chandelier_short and bars.iloc[-1]["close"] > self.cdlr.chandelier_short:
            self.close_position("触发吊灯止盈")
                
    def update(self, bars):
        self.bars = bars
        self.update_cdlr(bars)
        self.close_position_before_market_close()
        self.close_position_by_chandier_exit(bars)