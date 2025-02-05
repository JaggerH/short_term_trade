from utils import volatility
import pandas as pd

class PositionManager:
    def __init__(self, ib, debug=False):
        self.ib = ib
        self.debug = debug
        self.positions = []  # 存储多个合约的仓位信息
        self.trade_log = []  # 交易记录列表

    def find_position(self, symbol):
        return next((item for item in self.positions if item.get("symbol") == symbol), None)
        
    def has_position(self, symbol):
        """
        检查指定合约是否有仓位
        """
        return any(position["symbol"] == symbol for position in self.positions)

    def get_available_funds(self):
        if self.debug:
            return 1000000, 1000000
        account_summary = self.ib.accountSummary()
        df = pd.DataFrame(account_summary)

        result = df.set_index('tag')['value'].to_dict()

        net_liquidation = result.get('NetLiquidation', None) # 账户净资产
        available_funds = result.get('AvailableFunds', None) # 可用资金
        
        return net_liquidation, available_funds
    
    def calculate_open_amount(self, bars):
        net_liquidation, available_funds = self.get_available_funds()
        if net_liquidation is None or available_funds is None:
            print("PositionManager.calculate_open_amount net_liquidation or available_funds is None")
            return 0
        
        vol = volatility(bars['close'])
        # target_market_value = net_liquidation * 0.1 * vol * 1000
        # print(f'波动率:{vol}，目标市值:{target_market_value}')
        target_market_value = net_liquidation * 0.1
        if target_market_value > available_funds: return 0
        
        open_amount = target_market_value / bars.iloc[-1]['close']
        open_amount = round(open_amount / 10) * 10  # 调整为 10 的倍数
        return int(open_amount)
        
    def add_position(self, symbol, entry_price, amount, entry_time):
        """
        添加仓位到 positions 中
        """
        self.positions.append({
            "symbol": symbol,
            "price": entry_price,
            "amount": amount,
            "date": entry_time
        })

    def remove_position(self, symbol):
        """
        从 positions 中移除仓位
        """
        self.positions = [pos for pos in self.positions if pos["symbol"] != symbol]

    def log_trade(self, symbol, open_or_close, direction, amount, time):
        """
        记录交易信息
        """
        self.trade_log.append({
            "symbol": symbol,
            "open_or_close": open_or_close,
            "direction": direction,
            "amount": amount,
            "date": time
        })

    def open_position(self, symbol, direction, amount, entry_price, entry_time):
        """
        开仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        self.add_position(symbol, entry_price, amount, entry_time)
        self.log_trade(symbol, "开仓", direction, amount, entry_time)  # 记录交易
        if not self.debug:
            # 在非 debug 模式下调用 IBKR 的开仓 API
            self.ibkr_open(symbol, entry_price)

    def close_position(self, symbol, exit_time):
        """
        平仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        position = self.find_position(symbol)
        direction = "Bought" if position["amount"] < 0 else "Sold" # 因为要做反向操作
        amount = -1 * position["amount"]
        self.remove_position(symbol)
        self.log_trade(symbol, "平仓", direction, amount, exit_time)  # 记录交易
        if not self.debug:
            # 在非 debug 模式下调用 IBKR 的平仓 API
            self.ibkr_close(symbol)

    def ibkr_open(self, symbol, entry_price):
        """
        IBKR 开仓 API 的占位实现
        """
        print(f"调用 IBKR API 开仓: {symbol} @ {entry_price}")

    def ibkr_close(self, symbol):
        """
        IBKR 平仓 API 的占位实现
        """
        print(f"调用 IBKR API 平仓: {symbol}")

    def update(self, symbol, structure, bars, current_time):
        """
        更新仓位状态：根据是否有仓位执行开仓或平仓逻辑
        """
        # if self.has_position(symbol):
        #     position = self.find_position(symbol)
        #     amount      = position["amount"]
        #     entry_price = position["price"]
        #     entry_time  = position["date"]

        #     # 如果有仓位，检查是否发出平仓信号
        #     exit_signal = structure.cal_exit_signal(bars, amount, entry_price, entry_time)
        #     if exit_signal:
        #         self.close_position(symbol, current_time)
        #         exit_price = bars.iloc[-1]['close']
        #         pnl = (exit_price - entry_price) * amount
        #         print(f"【{current_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")

        # # 如果没有仓位，检查是否发出开仓信号
        # if not self.has_position(symbol):
        #     signal = structure.cal(bars)
        #     if signal:
        #         direction = "Bought" if signal == "底背离" else "Sold"
        #         # 获取当前价格和时间
        #         entry_price = bars.iloc[-1]["close"]
        #         amount = self.calculate_open_amount(bars)
        #         amount = (-1 if direction == "Sold" else 1) * amount
        #         self.open_position(symbol, direction, amount, entry_price, current_time)
        #         print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")
                
        ## -------------------------------------------
        signal = structure.cal(bars)
        if not self.has_position(symbol):
            if signal:
                direction = "Bought" if signal == "底背离" else "Sold"
                # 获取当前价格和时间
                entry_price = bars.iloc[-1]["close"]
                amount = self.calculate_open_amount(bars)
                amount = (-1 if direction == "Sold" else 1) * amount
                self.open_position(symbol, direction, amount, entry_price, current_time)
                print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")
        else:
            position = self.find_position(symbol)
            amount      = position["amount"]
            entry_price = position["price"]
            entry_time  = position["date"]

            # 如果有仓位，检查是否发出平仓信号
            exit_signal = structure.cal_exit_signal(bars, amount, entry_price, entry_time)
            
            if exit_signal and not signal:
                self.close_position(symbol, current_time)
                exit_price = bars.iloc[-1]['close']
                pnl = (exit_price - entry_price) * amount
                print(f"【{current_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")
                
            if exit_signal and signal:
                exit_amount = amount * -1 # 这是退出的数量及方向
                
                direction = "Bought" if signal == "底背离" else "Sold"
                new_open_amount = self.calculate_open_amount(bars)
                new_open_amount = (-1 if direction == "Sold" else 1) * new_open_amount
                if new_open_amount * exit_amount < 0:
                    raise("同时收到开仓和平仓方向，且方向不一致")
                else:
                    self.close_position(symbol, current_time)
                    exit_price = bars.iloc[-1]['close']
                    pnl = (exit_price - entry_price) * amount
                    print(f"【{current_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")
                    
                    entry_price = bars.iloc[-1]["close"]
                    amount = self.calculate_open_amount(bars)
                    amount = (-1 if direction == "Sold" else 1) * amount
                    self.open_position(symbol, direction, amount, entry_price, current_time)
                    print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")