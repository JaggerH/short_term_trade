from ib_insync import *
from utils import volatility
import pandas as pd
import time

ACCOUNT_REQUEST_INTERVAL = 60
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
        self.account_request_time = None # 上次request_account_summary的事件
        if ib:
            self.ib.orderStatusEvent += self.on_order_status
            self.ib.accountSummaryEvent += self.on_account_summary
            self.request_account_summary()
        else:
            self.net_liquidation = 1000000
            self.available_funds = 1000000
        
        self.test_count = 0
        
    def on_account_summary(self, account_summary):
        # AccountValue(account='All', tag='RealCurrency', value='BASE', currency='BASE', modelCode='')
        if account_summary.tag == "NetLiquidation": self.net_liquidation = float(account_summary.value)
        if account_summary.tag == "AvailableFunds": self.available_funds = float(account_summary.value)

    def request_account_summary(self):
        # 请求账户摘要信息
        if self.ib:
            current_time = time.time()  # 获取当前时间戳
            if self.account_request_time is not None and current_time - self.account_request_time < ACCOUNT_REQUEST_INTERVAL: return None
            self.ib.reqAccountSummaryAsync()
            self.account_request_time = current_time
        
    def find_position(self, symbol):
        return next((item for item in self.positions if item.get("symbol") == symbol), None)
        
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

    def calculate_open_amount(self, bars):
        # net_liquidation, available_funds = self.get_available_funds()
        self.request_account_summary()
        if self.net_liquidation is None or self.available_funds is None:
            print("PositionManager.calculate_open_amount net_liquidation or available_funds is None")
            return 0
        
        vol = volatility(bars['close'])
        # target_market_value = net_liquidation * 0.1 * vol * 1000
        # print(f'波动率:{vol}，目标市值:{target_market_value}')
        target_market_value = self.net_liquidation * 0.1
        if target_market_value > self.available_funds: return 0
        
        open_amount = target_market_value / bars.iloc[-1]['close']
        open_amount = round(open_amount / 10) * 10  # 调整为 10 的倍数
        return int(open_amount)
    
    def find_structure_open_trade(self, contract, amount, block_id):
        """
            Structure开仓交易的数量是变动的 不能用作查找开仓交易的标准
            比如在两个临近时点 分别触发开仓信号 但是开仓数量因为净值的波动会发生变化 无法查找到对应交易会导致重复开仓
            
            或者有更好的算法 open_position_to_amount?我觉得这不尽然 这涉及到不同策略间的信号汇总了
            根据position在ibkr_close_position中调用一定能找到一个正在交易的唯一值
            {
                "trade": instanceof trade,
                "entry_time": timestamp,
                "type": str 开仓或平仓,
                "strategy": str structure,
                "block_id": int,
                "callback": function
            }
        """
        action = 'BUY' if amount > 0 else 'SELL'
        amount = abs(amount)
        is_match = lambda item: (
            item["trade"].contract == contract and
            item["trade"].order.action == action and
            item["trade"].order.totalQuantity == amount and
            item["strategy"] == "structure" and
            item["block_id"] == block_id
        )
        return next((item for item in self.trades if is_match(item)), None)
    
    def find_close_trade(self, contract, amount):
        """
            因为平仓交易的合约、数量、方向是一致的
            根据position在ibkr_close_position中调用一定能找到一个正在交易的唯一值
            {
                "trade": instanceof trade,
                "entry_time": timestamp,
                "type": str 开仓或平仓,
                "callback": function
            }
        """
        action = 'BUY' if amount > 0 else 'SELL'
        amount = abs(amount)
        return next((item for item in self.trades if item["trade"].contract == contract and item["trade"].order.action == action and item["trade"].order.totalQuantity == amount), None)
    
    def find_trade_by_order_id(self, orderId):
        return next((item for item in self.trades if item["trade"].order.orderId == orderId), None)
        
    def add_trade(self, item):
        self.trades.append(item)
        
    def remove_trade(self, contract, amount):
        trade = self.find_trade(contract, amount)
        self.trades.remove(trade)
        
    def remove_trade_by_order_id(self, orderId):
        trade = self.find_trade_by_order_id(orderId)
        self.trades.remove(trade)
        
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
    
    def ibkr_open_position(self, contract, amount, entry_time, redundant_trade_info={}):
        def callback(trade, entry_time=entry_time):
            """
            回调函数，处理订单完成后的逻辑
            """
            if trade.orderStatus.status == 'Filled':
                print(f"订单完全成交: {trade.orderStatus.filled}股, 平均成交价: {trade.orderStatus.avgFillPrice}")
                self.add_position(contract.symbol, trade.orderStatus.avgFillPrice, trade.orderStatus.filled, entry_time)
                self.log_trade(contract.symbol, "开仓", trade.order.action, trade.orderStatus.filled, trade.fills[-1].time)  # 记录交易
                self.remove_trade_by_order_id(trade.order.orderId)
                
        trade = self.ibkr_trade(contract, amount)
        trade_dict = { "trade": trade, "entry_time": entry_time, "type": "开仓", "callback": callback }
        trade_dict = {**trade_dict, **redundant_trade_info}
        self.add_trade(trade_dict)
            
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
           
    def ibkr_close_position(self, contract, amount, exit_time, redundant_trade_info={}):
        amount = -1 * amount
        
        def callback(trade):
            if trade.orderStatus.status == 'Filled':
                self.log_trade(contract.symbol, "平仓",  trade.order.action, trade.orderStatus.filled, trade.fills[-1].time)  # 记录交易
                position = self.find_position(contract.symbol)    
                exit_price = trade.orderStatus.avgFillPrice
                entry_price = position["price"]    
                pnl = (exit_price - entry_price) * amount
                self.remove_position(contract.symbol)
                print(f"【{trade.fills[-1].time}】平仓: {contract.symbol}, 价格: {trade.orderStatus.filled}, 浮动盈亏：{pnl}")
                
                self.remove_trade_by_order_id(trade.order.orderId)
            
        trade = self.ibkr_trade(contract, amount)
        trade_dict = { "trade": trade, "entry_time": exit_time, "type": "平仓", "callback": callback }
        trade_dict = {**trade_dict, **redundant_trade_info}
        self.add_trade(trade_dict)
            
    def ibkr_trade(self, contract, amount):
        assert amount != 0
        direction = 'BUY' if amount > 0 else 'SELL'
        print(contract, direction, amount)
        order = MarketOrder(direction, abs(amount))
        
        trade = self.ib.placeOrder(contract, order)
        return trade

    def on_order_status(self, trade):
        """
        处理订单状态更新的回调函数
        """
        _trade = self.find_trade_by_order_id(trade.order.orderId)
        if _trade: _trade["callback"](trade)
            
    def structure_entry(self, bars, contract, signal, block_id):
        """
            Args:
                block_id:用于标记交易
        """
        symbol = contract.symbol
        direction = "Bought" if signal == "底背离" else "Sold"
        # 获取当前价格和时间
        entry_price = bars.iloc[-1]["close"]
        amount = self.calculate_open_amount(bars)
        amount = (-1 if direction == "Sold" else 1) * amount
        current_time = bars.iloc[-1]["date"]
        
        print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")
        if self.debug:
            self.debug_open_position(contract, direction, amount, entry_price, current_time)
        else:
            # 在非 debug 模式下调用 IBKR 的开仓 API，异步处理
            if not self.find_structure_open_trade(contract, amount, block_id):
                self.ibkr_open_position(contract, amount, current_time, {"strategy": "structure", "block_id": block_id})
        
    def structure_exit(self, bars, contract, exit_signal):
        symbol = contract.symbol
        position = self.find_position(symbol)
        amount      = position["amount"]
        entry_price = position["price"]
        exit_price  = bars.iloc[-1]["close"]
        current_time = bars.iloc[-1]["date"]
            
        if self.debug:
            self.debug_close_position(contract, current_time, entry_price, exit_price, exit_signal)
        else:
            if not self.find_close_trade(contract, amount):
                self.ibkr_close_position(contract, amount, current_time, {"strategy": "structure"})
                        
    def test_trade(self, bars, contract):
        # symbol = contract.symbol
        if self.test_count == 0:
            self.structure_entry(bars, contract, "底背离", 5)
        
        self.test_count += 1
        if self.test_count == 3:
            self.structure_exit(bars, contract, "MACD反向运行")
            
    def update(self, contract, structure, bars, current_time):
        """
        更新仓位状态：根据是否有仓位执行开仓或平仓逻辑
        """
        symbol = contract.symbol
        signal = structure.cal(bars)
        block_id = structure.data['block_id'].max()
        
        if not self.has_position(symbol):
            if signal:
                self.structure_entry(bars, contract, signal, block_id)
        else:
            position = self.find_position(symbol)
            amount      = position["amount"]
            entry_price = position["price"]
            entry_time  = position["date"]

            # 如果有仓位，检查是否发出平仓信号
            exit_signal = structure.cal_exit_signal(bars, amount, entry_price, entry_time)
            
            if exit_signal and not signal:
                self.structure_exit(bars, contract, exit_signal)
                
            if exit_signal and signal:
                exit_amount = amount * -1 # 这是退出的数量及方向
                
                direction = "Bought" if signal == "底背离" else "Sold"
                new_open_amount = self.calculate_open_amount(bars)
                new_open_amount = (-1 if direction == "Sold" else 1) * new_open_amount
                if new_open_amount * exit_amount < 0:
                    raise("同时收到开仓和平仓方向，且方向不一致")
                else:
                    # self.close_position(contract, current_time)
                    # exit_price = bars.iloc[-1]['close']
                    # pnl = (exit_price - entry_price) * amount
                    # print(f"【{current_time}】平仓: {symbol}, 价格: {exit_price}, 平仓原因：{exit_signal}, 浮动盈亏：{pnl}")
                    
                    # entry_price = bars.iloc[-1]["close"]
                    # amount = self.calculate_open_amount(bars)
                    # amount = (-1 if direction == "Sold" else 1) * amount
                    # self.open_position(contract, direction, amount, entry_price, current_time)
                    # print(f"【{current_time}】开仓: {symbol}, 方向： {direction}, 数量： {amount}, 价格: {entry_price}, 预估市值：{amount * entry_price}")
                    self.structure_exit(bars, contract, exit_signal, current_time)
                    self.structure_entry(bars, contract, signal, current_time)
                    