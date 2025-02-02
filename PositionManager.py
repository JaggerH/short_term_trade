class PositionManager:
    def __init__(self, debug=False):
        self.debug = debug
        self.positions = []  # 存储多个合约的仓位信息
        self.trade_log = []  # 交易记录列表

    def has_position(self, symbol):
        """
        检查指定合约是否有仓位
        """
        return any(position["symbol"] == symbol for position in self.positions)

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
        # print(f"开仓: {symbol}, 方向： {direction}, 价格: {entry_price}, 时间: {entry_time}")

    def close_position(self, symbol, exit_time):
        """
        平仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        position = next((item for item in self.positions if item.get("symbol") == symbol), None)
        direction = "Bought" if position["amount"] < 0 else "Sold" # 因为要做反向操作
        self.remove_position(symbol)
        self.log_trade(symbol, "平仓", direction, position["amount"], exit_time)  # 记录交易
        if not self.debug:
            # 在非 debug 模式下调用 IBKR 的平仓 API
            self.ibkr_close(symbol)
        # print(f"平仓: {symbol}, 时间: {exit_time}")

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

    def update(self, structure, bars, current_time):
        """
        更新仓位状态：根据是否有仓位执行开仓或平仓逻辑
        """
        for position in self.positions:
            symbol  = position["symbol"]
            amount  =  position["amount"]
            entry_price = position["price"]
            entry_time  = position["date"]

            # 如果有仓位，检查是否发出平仓信号
            exit_signal = structure.cal_exit_signal(bars, amount, entry_price, entry_time)
            if exit_signal:
                self.close_position(symbol, current_time)
                print(exit_signal)
                # print(f"平仓: {symbol}, 价格: {bars.iloc[-1]['close']}, 时间：{current_time}")

        # 如果没有仓位，检查是否发出开仓信号
        monitored_symbol = "某股票"  # 假设监控 "某股票"
        if not self.has_position(monitored_symbol):
            signal = structure.cal(bars)
            if signal:
                direction = "Bought" if signal == "底背离" else "Sold"
                # 获取当前价格和时间
                entry_price = bars.iloc[-1]["close"]
                amount = (-1 if direction == "Sold" else 1) * 100
                self.open_position(monitored_symbol, direction, amount, entry_price, current_time)
