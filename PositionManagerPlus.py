from ib_insync import *

import dill
import yaml
import redis

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
        if ib:
            self.ib.orderStatusEvent += self.on_order_status
            self.ib.accountSummaryEvent += self.on_account_summary
            self.ib.reqAccountSummaryAsync()
        else:
            self.net_liquidation = 1000000
            self.available_funds = 1000000
        
        if not self.debug:
            self.restore()
        
    def on_account_summary(self, account_summary):
        if account_summary.tag == "NetLiquidation": self.net_liquidation = float(account_summary.value)
        if account_summary.tag == "AvailableFunds": self.available_funds = float(account_summary.value)
        
    def on_order_status(self, trade):
        """
        处理订单状态更新的回调函数
        """
        _trade = self.find_trade_by_order_id(trade.order.orderId)
        if _trade: _trade["callback"](self, trade)

    def get_redis(self):
        if not self._redis:
            # 加载配置
            with open("config.yml", "r") as file:
                config = yaml.safe_load(file)

            redis_config = config.get("redis", {})
            self._redis = redis.Redis(**redis_config)
        return self._redis
    
    def save(self):
        redis_client = self.get_redis()
        data = {
            "positions": self.positions,
            "trade_log": self.trade_log,
            "trades": self.trades
        }
        redis_client.set("position_manager_data", dill.dumps(data))
        
    def restore(self):
        redis_client = self.get_redis()
        data = redis_client.get("position_manager_data")
        if data:
            data = dill.loads(data)
            self.positions  = data.get("positions", [])
            self.trade_log  = data.get("trade_log", [])
            self.trades     = data.get("trades", [])
            
            complete_orders = self.ib.reqCompletedOrders(True)
            for trade in complete_orders:
                trade = self.find_trade_by_order_id(trade.order.orderId)
                if trade: trade["callback"](self, trade)
            
    def clear_redis(self):
        redis_client = self.get_redis()
        redis_client.delete("position_manager_data")
        
    def find_position(self, is_match):
        """
            Fields in position:
            - contract
            - strategy
            - price
            - amount
            - date
            
            is_match need to be a lamda or a function
            e.g.
            is_match = lambda item: (
                item["contract"] == contract and
                item["strategy"] == "RBreak" and
                item["action"] == action
            )
        """
        return next((item for item in self.positions if is_match(item)), None)
        
    def add_position(self, contract, strategy, price, amount, date):
        self.positions.append({
            "contract": contract.symbol,
            "price": price,
            "strategy": strategy,
            "amount": amount,
            "date": date
        })

    def remove_position(self, position):
        self.positions.remove(position)

    def find_trade(self, is_match):
        """
            Fields in trade:包括但不限于
            - trade     instanceof IBKR Trade, includes Fields blew
                - contract
                - order
                    - orderId
                    - action    BUY or SELL
                    - totalQuantity
            - strategy
            - open_or_close     开仓 or 平仓
            - date
            - callback
            
            is_match need to be a lamda or a function
            e.g.
            is_match = lambda item: (
                item["contract"] == contract and
                item["strategy"] == "RBreak" and
                item["action"] == action
            )
        """
        return next((item for item in self.trades if is_match(item)), None)
    
    def find_trade_by_order_id(self, orderId):
        is_match = lambda item: ( item["trade"].order.orderId == orderId )
        return self.find_trade(is_match)
        
    def add_trade(self, trade, strategy, open_or_close, date, callback):
        self.trades.append({
            "trade": trade,
            "strategy": strategy, 
            "open_or_close": open_or_close, 
            "date": date, 
            "callback": callback
        })
        
    def remove_trade(self, trade):
        self.trades.remove(trade)
    
    def remove_trade_by_order_id(self, orderId):
        is_match = lambda item: ( item["trade"].order.orderId == orderId )
        trade = self.find_trade(is_match)
        self.remove_trade(trade)
        
    def log(self, contract, strategy, open_or_close, direction, price, amount, date):
        """
        记录交易信息
        """
        self.trade_log.append({
            "symbol": contract.symbol,
            "strategy": strategy,
            "open_or_close": open_or_close,
            "direction": direction,
            "price": price,
            "amount": amount,
            "date": date
        })

    def open_position(self, contract, strategy, amount, bars, allow_repeat_order = False):
        if self.debug:
            self.debug_open_position(contract, strategy, amount, bars.iloc[-1]['close'], bars.iloc[-1]['date'])
        else:
            is_match = lambda item: (
                item["trade"].contract == contract and
                item["strategy"] == strategy and
                item["open_or_close"] == "开仓"
            )
            if not allow_repeat_order and not self.find_trade(is_match):
                self.ibkr_open_position(contract, strategy, amount, bars.iloc[-1]['date'])
            
    def debug_open_position(self, contract, direction, amount, price, date):
        """
        开仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        self.add_position(contract, price, amount, date)
        self.log(contract, "开仓", direction, amount, date)  # 记录交易
    
    def ibkr_open_position(self, contract, strategy, amount, date, redundant_trade_info={}):
        def callback(self, trade, strategy=strategy):
            """
            回调函数，处理订单完成后的逻辑
            """
            direction = 1 if trade.order.action == 'BUY' else -1
            if trade.orderStatus.status == 'Filled':
                print(f"订单完全成交: {trade.orderStatus.filled}股, 平均成交价: {trade.orderStatus.avgFillPrice}")
                self.add_position(contract, strategy, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time)
                self.log(contract, strategy, "开仓", trade.order.action, trade.orderStatus.avgFillPrice, trade.orderStatus.filled, trade.fills[-1].time)  # 记录交易
                self.remove_trade_by_order_id(trade.order.orderId)
            if trade.orderStatus.status == 'Cancelled':
                print(f"订单已取消，成交: {trade.orderStatus.filled}股, 平均成交价: {trade.orderStatus.avgFillPrice}")
                if trade.orderStatus.filled != 0:
                    self.add_position(contract, strategy, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time)
                    self.log(contract, strategy, "开仓", trade.order.action, trade.orderStatus.avgFillPrice, trade.orderStatus.filled, trade.fills[-1].time)  # 记录交易    
                self.remove_trade_by_order_id(trade.order.orderId)
                
        trade = self.ibkr_trade(contract, amount)
        self.add_trade(trade, strategy, "开仓", date, callback)
    
    def close_position(self, contract, strategy, bars):
        if self.debug:
            self.debug_close_position(contract, strategy, bars)
        else:
            is_match = lambda item: (
                item["trade"].contract == contract and
                item["strategy"] == strategy and
                item["open_or_close"] == "平仓"
            )
            if not self.find_trade(is_match):
                self.ibkr_close_position(contract, strategy)

    def debug_close_position(self, contract, strategy, bars):
        is_match = lambda item: ( item["contract"] == contract and item["strategy"] == strategy )
        position = self.find_position(is_match)
        close_amount = -1 * position["amount"]
        direction = "SELL" if close_amount < 0 else "BUY" # 因为要做反向操作
        
        self.remove_position(position)
        self.log(contract, strategy, "平仓", direction, bars.iloc[-1]["close"], close_amount, bars.iloc[-1]["date"])  # 记录交易
        pnl = (bars.iloc[-1]["close"] - position["price"]) * close_amount
        print(f"【{bars.iloc[-1]["date"]}】平仓: {contract.symbol}, 价格: {bars.iloc[-1]["close"]}, 浮动盈亏：{pnl}")
           
    def ibkr_close_position(self, contract, strategy, bars):
        is_match = lambda item: ( item["contract"] == contract and item["strategy"] == strategy )
        position = self.find_position(is_match)
        close_amount = -1 * position["amount"]
        
        def callback(self, trade, strategy=strategy):
            direction = 1 if trade.order.action == 'BUY' else -1
            if trade.orderStatus.status == 'Filled':
                is_match = lambda item: ( item["contract"] == trade.contract and item["strategy"] == strategy )
                position = self.find_position(is_match)
                pnl = (trade.orderStatus.avgFillPrice - position["price"]) * trade.orderStatus.filled
                
                self.remove_position(position)
                self.log(contract, strategy, "平仓", trade.order.action, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time)  # 记录交易
                print(f"【{trade.fills[-1].time}】平仓: {contract.symbol}, 价格: {trade.orderStatus.avgFillPrice}, 浮动盈亏：{pnl}")
                self.remove_trade_by_order_id(trade.order.orderId)

        trade = self.ibkr_trade(contract, close_amount)
        self.add_trade(trade, strategy, "平仓", bars.iloc[-1]["date"], callback)
        
    def ibkr_trade(self, contract, amount):
        assert amount != 0
        direction = 'BUY' if amount > 0 else 'SELL'
        print(contract, direction, amount)
        order = MarketOrder(direction, abs(amount))
        order.outsideRth = True  # 允许在非常规交易时段执行
        trade = self.ib.placeOrder(contract, order)
        return trade