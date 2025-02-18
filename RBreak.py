import pandas as pd
from PositionManagerPlus import PositionManager
STOP_LOSS_PERCENT = 0.02

class RBreak:
    def __init__(self, ib, contract, pm: PositionManager):
        self.ib = ib
        self.contract = contract
        self.pm = pm
        
        self.open_position_price = None
        self.debug = True
        
    def setParams(self, df=None, date=None):
        """
        Args:
            df: 日线的OHLC
        """
        if df is None:
            bars = self.ib.reqHistoricalData(
                self.contract,
                endDateTime=date,  # 指定日期（美东时间）
                durationStr='1 D',  # 仅获取1天数据
                barSizeSetting='1 day',  # 日线
                whatToShow='TRADES',  # 成交价
                useRTH=1,  # 仅常规交易时间
                formatDate=1
            )
            df = pd.DataFrame(bars)
        high = df.iloc[-1]['high']  # 前一日的最高价
        low = df.iloc[-1]['low']  # 前一日的最低价
        close = df.iloc[-1]['close']  # 前一日的收盘价
        
        pivot = (high + low + close) / 3  # 枢轴点
        self.bBreak = high + 2 * (pivot - low)  # 突破买入价
        self.sSetup = pivot + (high - low)  # 观察卖出价
        self.sEnter = 2 * pivot - low  # 反转卖出价
        self.bEnter = 2 * pivot - high  # 反转买入价
        self.bSetup = pivot - (high - low)  # 观察买入价
        self.sBreak = low - 2 * (high - pivot)  # 突破卖出价
    
    def calculate_open_amount(self, bars):
        # net_liquidation, available_funds = self.get_available_funds()
        if self.pm.net_liquidation is None or self.pm.available_funds is None:
            print("PositionManager.calculate_open_amount net_liquidation or available_funds is None")
            return 0
        
        # target_market_value = net_liquidation * 0.1 * vol * 1000
        # print(f'波动率:{vol}，目标市值:{target_market_value}')
        target_market_value = self.pm.net_liquidation * 0.1
        if target_market_value > self.pm.available_funds: return 0
        
        open_amount = target_market_value / bars.iloc[-1]['close']
        open_amount = round(open_amount / 10) * 10  # 调整为 10 的倍数
        return int(open_amount)
    
    def find_position(self, action):
        is_match = None
        if action == "BUY":
            is_match = lambda item: ( item["contract"] == self.contract and item["strategy"] == "RBreak" and item["amount"] > 0 )
        else:
            is_match = lambda item: ( item["contract"] == self.contract and item["strategy"] == "RBreak" and item["amount"] < 0 )
        return self.pm.find_position(is_match)
                
    def update(self, bars):
        # 获取现有持仓
        position_long   = self.find_position("BUY")
        position_short  = self.find_position("SELL")
        
        # 突破策略:
        if not position_long and not position_short:  # 空仓条件下
            if bars.iloc[-1]["close"] > self.bBreak:
                # 在空仓的情况下，如果盘中价格超过突破买入价，则采取趋势策略，即在该点位开仓做多
                amount = self.calculate_open_amount(bars)
                self.pm.open_position(self.contract, "RBreak", amount, bars)
                print(f"{bars.iloc[-1]['date']}空仓,盘中价格超过突破买入价: 开仓做多")
                self.open_position_price = bars.iloc[-1]["close"]
            elif bars.iloc[-1]["close"] < self.sBreak:
                # 在空仓的情况下，如果盘中价格跌破突破卖出价，则采取趋势策略，即在该点位开仓做空
                amount = -1 * self.calculate_open_amount(bars)
                self.pm.open_position(self.contract, "RBreak", amount, bars)
                print(f"{bars.iloc[-1]['date']}空仓,盘中价格跌破突破卖出价: 开仓做空")
                self.open_position_price = bars.iloc[-1]["close"]
        # 设置止损条件
        else:  # 有持仓时
            change_percent = (bars.iloc[-1]["close"] - self.open_position_price) / self.open_position_price
            # 开仓价与当前行情价之差大于止损点则止损
            if (position_long and change_percent <= -1 * STOP_LOSS_PERCENT) or \
                    (position_short and change_percent >= STOP_LOSS_PERCENT):
                print(f'{bars.iloc[-1]["date"]}达到止损点，全部平仓')
                self.pm.close_position(self.contract, "RBreak", bars)  # 平仓
            # 反转策略:
            if position_long:  # 多仓条件下
                if bars['high'].max() > self.sSetup and bars.iloc[-1]["close"] < self.sEnter:
                    # 多头持仓,当日内最高价超过观察卖出价后，
                    # 盘中价格出现回落，且进一步跌破反转卖出价构成的支撑线时，
                    # 采取反转策略，即在该点位反手做空
                    self.pm.close_position(self.contract, "RBreak", bars)  # 平仓
                    amount = -1 * self.calculate_open_amount(bars)
                    self.pm.open_position(self.contract, "RBreak", amount, bars)
                    print(f"{bars.iloc[-1]['date']}多头持仓,当日内最高价超过观察卖出价后跌破反转卖出价: 反手做空")
                    self.open_position_price = bars.iloc[-1]["close"]
            elif position_short:  # 空头持仓
                if bars['low'].min() < self.bSetup and bars.iloc[-1]["close"] > self.bEnter:
                    # 空头持仓，当日内最低价低于观察买入价后，
                    # 盘中价格出现反弹，且进一步超过反转买入价构成的阻力线时，
                    # 采取反转策略，即在该点位反手做多
                    self.pm.close_position(self.contract, "RBreak", bars)  # 平仓
                    amount = self.calculate_open_amount(bars)
                    self.pm.open_position(self.contract, "RBreak", amount, bars)
                    print(f"{bars.iloc[-1]['date']}空头持仓,当日最低价低于观察买入价后超过反转买入价: 反手做多")
                    self.open_position_price = bars.iloc[-1]["close"]
        # if self.now.hour == 14 and self.now.minute == 59:
        #     self.pm.close_position(self.contract, "RBreak", bars)
        #     print('全部平仓')