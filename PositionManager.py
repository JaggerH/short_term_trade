from ib_insync import *
from utils import volatility
import pandas as pd
import time
import threading

class PositionManager:
    """
        debug模式下不需要ibkr参与计算
        计算过程仅采用提供的行情数据
        成交模式改为即时成交
    """
    def __init__(self, ib, debug=False):
        self.ib = ib
        self.debug = debug
        self.positions = []  # 存储多个合约的仓位信息
        self.trade_log = []  # 交易记录列表
        self.trades    = []  # 记录下单中的交易

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

        net_liquidation = result.get('NetLiquidation', 0) # 账户净资产
        available_funds = result.get('AvailableFunds', 0) # 可用资金
        net_liquidation = float(net_liquidation)
        available_funds = float(available_funds)
        
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

    def add_trade(self, trade):
        self.trades.append(trade)
        
    def remove_trade(self, contract, amount):
        trade = self.find_trade(contract, amount)
        self.trades.remove(trade)
        
    def remove_trade(self, trade):
        self.trades.remove(trade)
        
    def find_trade(self, contract, amount):
        action = 'BUY' if amount > 0 else 'SELL'
        amount = abs(amount)
        return next((trade for trade in self.trades if trade.contract == contract and trade.order.action == action and trade.order.totalQuantity == amount), None)
    
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

    def debug_open_position(self, contract, direction, amount, entry_price, entry_time):
        """
        开仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        symbol = contract.symbol

        self.add_position(symbol, entry_price, amount, entry_time)
        self.log_trade(symbol, "开仓", direction, amount, entry_time)  # 记录交易
        print(f"【{entry_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")

    def ibkr_open_position(self, contract, amount, entry_time):
        if not self.find_trade(contract, amount):
            trade = self.ibkr_trade(contract, amount)
            self.add_trade(trade)
            trade = self.wait_ibkr_trade(trade)
            filled_amount = trade.orderStatus.filled
            if filled_amount > 0:
                self.add_position(contract.symbol, trade.orderStatus.avgFillPrice, trade.orderStatus.filled, entry_time)
            self.remove_trade(trade)
            
    def debug_close_position(self, contract, exit_time, entry_price, exit_price, exit_signal):
        """
        平仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        symbol = contract.symbol
        position = self.find_position(symbol)
        amount = -1 * position["amount"]
        direction = "Bought" if position["amount"] < 0 else "Sold" # 因为要做反向操作
        
        self.remove_position(symbol)
        self.log_trade(symbol, "平仓", direction, amount, exit_time)  # 记录交易
        pnl = (exit_price - entry_price) * amount
        print(f"【{exit_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")
        
    def ibkr_close_position(self, contract, amount, exit_time):
        position = self.find_position(contract.symbol)
        direction = "Bought" if position["amount"] < 0 else "Sold" # 因为要做反向操作
        
        if not self.find_trade(contract, amount):
            trade = self.ibkr_trade(contract, amount)
            self.add_trade(trade)
            self.wait_ibkr_trade(trade, cancelOrder=False)
            self.remove_trade(trade)
            self.log_trade(contract.symbol, "平仓", direction, trade.orderStatus.filled, trade.orderStatus.log[-1].time, exit_time)  # 记录交易
            
    def ibkr_trade(self, contract, amount):
        assert amount != 0
        direction = 'BUY' if amount > 0 else 'SELL'
        print(contract, direction, amount)
        order = MarketOrder(direction, abs(amount))
        
        trade = self.ib.placeOrder(contract, order)
        return trade

    def wait_ibkr_trade(self, trade, cancelOrder=True, cancelTime=30):
        """
        下单并监听订单状态，支持超时取消和部分成交。
        """        
        # 设置超时限制，等待30秒检查订单状态
        if cancelOrder:
            timeout = time.time() + cancelTime  # 当前时间 + 30秒
            while time.time() < timeout:
                self.ib.sleep(1)  # 每秒检查一次订单状态
                if trade.orderStatus.status == 'Filled':
                    print(f"订单完全成交: {trade.orderStatus.filled}股{trade.contract.symbol}")
                    return trade  # 完全成交，返回trade
                elif trade.orderStatus.status == 'Cancelled':
                    return trade  # 订单已取消，返回trade
                elif trade.orderStatus.status == 'PartiallyFilled':
                    print(f"订单部分成交: {trade.orderStatus.filled}股{trade.contract.symbol}")

            # 超过30秒后，如果仍未成交，则取消订单
            self.ib.cancelOrder(trade.order)  # 取消订单
            print(f"{trade.contract.symbol}订单超时取消，订单状态: {trade.orderStatus.status}, 已成交{trade.orderStatus.filled}股")
            
            return trade  # 返回订单，即使它被取消或部分成交
        else:
            while True:
                self.ib.sleep(1)  # 每秒检查一次订单状态
                if trade.orderStatus.status == 'Filled':
                    print(f"订单完全成交: {trade.orderStatus.filled}股{trade.contract.symbol}")
                    return trade  # 完全成交，返回trade
    
    def structure_entry(self, bars, contract, signal, current_time):
        symbol = contract.symbol
        direction = "Bought" if signal == "底背离" else "Sold"
        # 获取当前价格和时间
        entry_price = bars.iloc[-1]["close"]
        amount = self.calculate_open_amount(bars)
        amount = (-1 if direction == "Sold" else 1) * amount
        if self.debug:
            self.debug_open_position(contract, direction, amount, entry_price, current_time)
        else:
            # 在非 debug 模式下调用 IBKR 的开仓 API，异步处理
            trade_thread = threading.Thread(target=self.ibkr_open_position, args=(contract, amount, current_time))
            trade_thread.start()
        
    def structure_exit(self, bars, contract, exit_signal, current_time):
        symbol = contract.symbol
        position = self.find_position(symbol)
        amount      = position["amount"]
        entry_price = position["price"]
        exit_price  = bars.iloc[-1]["close"]
            
        if self.debug:
            self.debug_close_position(contract, current_time, entry_price, exit_price, exit_signal)
        else:
            trade_thread = threading.Thread(target=self.ibkr_close_position, args=(contract, amount, current_time))
            trade_thread.start()
            
    def update(self, contract, structure, bars, current_time):
        """
        更新仓位状态：根据是否有仓位执行开仓或平仓逻辑
        """
        symbol = contract.symbol
        signal = structure.cal(bars)
        if not self.has_position(symbol):
            if signal:
                self.structure_entry(bars, contract, signal, current_time)
        else:
            position = self.find_position(symbol)
            amount      = position["amount"]
            entry_price = position["price"]
            entry_time  = position["date"]

            # 如果有仓位，检查是否发出平仓信号
            exit_signal = structure.cal_exit_signal(bars, amount, entry_price, entry_time)
            
            if exit_signal and not signal:
                self.structure_exit(bars, contract, exit_signal, current_time)
                
            if exit_signal and signal:
                exit_amount = amount * -1 # 这是退出的数量及方向
                
                direction = "Bought" if signal == "底背离" else "Sold"
                new_open_amount = self.calculate_open_amount(bars)
                new_open_amount = (-1 if direction == "Sold" else 1) * new_open_amount
                if new_open_amount * exit_amount < 0:
                    raise("同时收到开仓和平仓方向，且方向不一致")
                else:
                    self.close_position(contract, current_time)
                    exit_price = bars.iloc[-1]['close']
                    pnl = (exit_price - entry_price) * amount
                    print(f"【{current_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")
                    
                    entry_price = bars.iloc[-1]["close"]
                    amount = self.calculate_open_amount(bars)
                    amount = (-1 if direction == "Sold" else 1) * amount
                    self.open_position(contract, direction, amount, entry_price, current_time)
                    print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")