from ib_insync import *

import pandas as pd
import dill
import yaml
import redis
import time

ACCOUNT_REQUEST_INTERVAL = 60
TEST_COMMISSION_PERCENT = 0.00008 # 测试手续费设置
SLIPPAGE = 0.002 # 滑点

class PositionManager:
    """
        debug模式下不需要ibkr参与计算
        计算过程仅采用提供的行情数据
        成交模式改为即时成交
    """
    def __init__(self, ib, strategy, debug=False, config_file="config.yaml"):
        self.ib = ib
        self.strategy = strategy # 用在存储策略
        self.debug = debug
        self.positions = []  # 存储多个合约的仓位信息
        self.trade_log = []  # 交易记录列表
        self.trades    = []  # 记录下单中的交易
        self.config_file = config_file
        if ib:
            # self.ib.orderStatusEvent += self.on_order_status
            self.ib.accountSummaryEvent += self.on_account_summary
            self.ib.commissionReportEvent += self.on_commission_report
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

    def on_commission_report(self, trade, fill, commissionReport):
        """
        处理佣金报告
        """
        # print(f"Commission Report Received")
        # print('Trade 1', trade)
        # print('Fill 1', fill)
        # print('commissionReport 1', commissionReport)
        # 在这里可以根据需要处理佣金和盈亏数据
        # 例如，你可以将它们存储到数据库或进一步计算
        _trade = self.find_trade_by_order_id(trade.order.orderId)
        if _trade: _trade["callback"](self, trade)
        
    def get_redis(self):
        if not hasattr(self, '_redis'):
            # 加载配置
            with open(self.config_file, "r") as file:
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
        redis_client.set(f"{self.strategy}_position_manager", dill.dumps(data))
        
    def restore(self):
        redis_client = self.get_redis()
        data = redis_client.get(f"{self.strategy}_position_manager")
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
        redis_client.delete(f"{self.strategy}_position_manager")
        
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
            "contract": contract,
            "price": price,
            "strategy": strategy,
            "amount": amount,
            "init_amount": amount,
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
        
    def log(self, contract, strategy, open_or_close, direction, price, amount, date, commission, pnl=None, reason=None):
        """
        记录交易信息
        """
        self.trade_log.append({
            "date": date,
            "symbol": contract.symbol,
            "strategy": strategy,
            "open_or_close": open_or_close,
            "direction": direction,
            "price": price,
            "amount": amount,
            "commission": commission,
            "pnl": pnl,
            "reason": reason
        })
        print(f'【{date}】【{strategy}】{open_or_close}: {contract.symbol}, 价格: {price}, 数量：{amount}，浮动盈亏：{pnl}, 原因：{reason}')
        if not self.debug: self.save()

    def open_position(self, contract, strategy, amount, bars, reason=None, allow_repeat_order = False):
        if self.debug:
            if amount != 0:
                self.debug_open_position(contract, strategy, amount, bars.iloc[-1]['close'], bars.iloc[-1]['date'], reason=reason)
        else:
            is_match = lambda item: (
                item["trade"].contract == contract and
                item["strategy"] == strategy and
                item["open_or_close"] == "开仓"
            )
            if not allow_repeat_order and not self.find_trade(is_match):
                self.ibkr_open_position(contract, strategy, amount, bars.iloc[-1]['date'], reason=reason)
    
    def debug_trade_price_slippage(self, amount, bars):
        close = bars.iloc[-1]['close']
        price = 0
        if amount > 0: 
            # 买入价格，增加买入价
            price = close * (1 + SLIPPAGE)
        else:
            price = close * (1 - SLIPPAGE)
        return price
    
    def debug_open_position(self, contract, strategy, amount, price, date, reason=None):
        """
        开仓操作：修改仓位 + 调用 IBKR API（如果不处于 debug 模式）
        """
        direction = 'BUY' if amount > 0 else 'SELL'
        self.add_position(contract, strategy, price, amount, date)
        commission = abs(amount * price) * TEST_COMMISSION_PERCENT
        self.log(contract, strategy, "开仓", direction, price, amount, date, commission, reason=reason)  # 记录交易

        # 更新 available_funds，扣除开仓所需的资金（包含佣金）
        self.available_funds -= abs(amount * price) + commission
        
    def ibkr_open_position(self, contract, strategy, amount, date, reason=None, redundant_trade_info={}):
        def callback(self, trade, strategy=strategy):
            """
            回调函数，处理订单完成后的逻辑
            """
            direction = 1 if trade.order.action == 'BUY' else -1
            if trade.orderStatus.status in ['Filled', 'Cancelled']:
                commission, pnl = self.get_commission_and_pnl_from_fills(trade)
                if trade.orderStatus.filled != 0:
                    self.add_position(contract, strategy, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time)
                    self.log(contract, strategy, "开仓", trade.order.action, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time, commission, reason=reason)  # 记录交易    
                self.remove_trade_by_order_id(trade.order.orderId)
                
        trade = self.ibkr_trade(contract, amount)
        self.add_trade(trade, strategy, "开仓", date, callback)
    
    def close_position(self, position, bars, reason=None):
        if self.debug:
            self.debug_close_position(position, bars, reason=reason)
        else:
            is_match = lambda item: (
                item["trade"].contract == position["contract"] and
                item["strategy"] == position["strategy"] and
                item["open_or_close"] == "平仓"
            )
            if not self.find_trade(is_match):
                self.ibkr_close_position(position, bars, reason=reason)

    def debug_close_position(self, position, bars, reason=None):
        close_amount = -1 * position["amount"]
        direction = "SELL" if close_amount < 0 else "BUY" # 因为要做反向操作
        pnl = (bars.iloc[-1]["close"] - position["price"]) * position["amount"]
        
        self.remove_position(position)
        commission = abs(close_amount * bars.iloc[-1]["close"]) * TEST_COMMISSION_PERCENT
        self.log(position["contract"], position["strategy"], "平仓", direction, bars.iloc[-1]["close"], close_amount, bars.iloc[-1]["date"], commission, pnl, reason=reason)  # 记录交易
        self.available_funds += abs(position["amount"] * position["price"]) + pnl - commission
            
    def ibkr_close_position(self, position, bars, reason=None):
        close_amount = -1 * position["amount"]
        
        def callback(self, trade, position=position):
            direction = 1 if trade.order.action == 'BUY' else -1
            if trade.orderStatus.status == 'Filled':
                commission, pnl = self.get_commission_and_pnl_from_fills(trade)
                
                self.remove_position(position)
                self.log(position["contract"], position["strategy"], "平仓", trade.order.action, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time, commission, pnl, reason=reason)  # 记录交易
                self.remove_trade_by_order_id(trade.order.orderId)

        trade = self.ibkr_trade(position["contract"], close_amount)
        self.add_trade(trade, position["strategy"], "平仓", bars.iloc[-1]["date"], callback)

    def substract_position(self, position, substract_percent, bars, reason=None):
        if self.debug:
            self.debug_substract_position(position, substract_percent, bars, reason=reason)
        else:
            is_match = lambda item: (
                item["trade"].contract == position["contract"] and
                item["strategy"] == position["strategy"] and
                item["open_or_close"] == "减仓"
            )
            if not self.find_trade(is_match):
                self.ibkr_substract_position(position, substract_percent, bars, reason=reason)

    def debug_substract_position(self, position, substract_percent, bars, reason=None):
        substract_amount = -1 * (position["init_amount"] * substract_percent)
        direction = "SELL" if substract_amount < 0 else "BUY" # 因为要做反向操作
        pnl = (bars.iloc[-1]["close"] - position["price"]) * substract_amount           
        commission = abs(substract_amount * bars.iloc[-1]["close"]) * TEST_COMMISSION_PERCENT
        self.log(position["contract"], position["strategy"], "减仓", direction, bars.iloc[-1]["close"], substract_amount, bars.iloc[-1]["date"], commission, pnl, reason=reason)  # 记录交易
        self.available_funds += abs(substract_amount * position["price"]) + pnl - commission
        
        position_index = self.positions.index(position)
        self.positions[position_index]["amount"] += substract_amount
        if self.positions[position_index]["amount"] == 0:
            self.remove_position(position)
        
    def ibkr_substract_position(self, position, substract_percent, bars, reason=None):
        substract_amount = -1 * (position["init_amount"] * substract_percent)
        
        def callback(self, trade, position=position):
            direction = 1 if trade.order.action == 'BUY' else -1
            if trade.orderStatus.status == 'Filled':
                commission, pnl = self.get_commission_and_pnl_from_fills(trade)
                self.log(position["contract"], position["strategy"], "减仓", trade.order.action, trade.orderStatus.avgFillPrice, direction * trade.orderStatus.filled, trade.fills[-1].time, commission, pnl, reason=reason)  # 记录交易
                self.remove_trade_by_order_id(trade.order.orderId)
                
                position_index = self.positions.index(position)
                self.positions[position_index]["amount"] += direction * trade.orderStatus.filled
                if self.positions[position_index]["amount"] == 0:
                    self.remove_position(position)

        trade = self.ibkr_trade(position["contract"], substract_amount)
        self.add_trade(trade, position["strategy"], "减仓", bars.iloc[-1]["date"], callback)
       
    def ibkr_trade(self, contract, amount):
        assert amount != 0
        direction = 'BUY' if amount > 0 else 'SELL'
        order = MarketOrder(direction, abs(amount))
        order.outsideRth = True  # 允许在非常规交易时段执行
        trade = self.ib.placeOrder(contract, order)
        return trade
    
    def calculate_open_amount(self, bars):
        # net_liquidation, available_funds = self.get_available_funds()
        if self.net_liquidation is None or self.available_funds is None:
            print("PositionManager.calculate_open_amount net_liquidation or available_funds is None")
            return 0
        
        # vol = volatility(bars['close'])
        # target_market_value = net_liquidation * 0.1 * vol * 1000
        # print(f'波动率:{vol}，目标市值:{target_market_value}')
        target_market_value = self.net_liquidation * 0.33
        if target_market_value > self.available_funds: return 0
        
        open_amount = target_market_value / bars.iloc[-1]['close']
        open_amount = round(open_amount / 10) * 10  # 调整为 10 的倍数
        return int(open_amount)
    
    def get_commission_and_pnl_from_fills(self, trade):
        """
        df be like:
            execId	                commission	currency	realizedPNL	yield_	yieldRedemptionDate
        0	00025b46.67b93a1f.01.01	1.0035	    USD	        -4.959198	0.0	    0
        1	00025b46.67b93a20.01.01	0.0035	    USD	        -3.959198	0.0	    0
        2	00025b46.67b93a21.01.01	1.0070	    USD	        -8.918396	0.0	    0
        3	00025b46.67b93a29.01.01	0.5035	    USD	        -4.459198	0.0	    0
        4	00025b46.67b93a2e.01.01	0.5035	    USD	        -4.459198	0.0	    0
        5	00025b46.67b93a2f.01.01	0.2014	    USD	        -1.783679	0.0	    0
        """
        commissionReports = [fill.commissionReport for fill in trade.fills]
        df = pd.DataFrame(commissionReports)
        return df["commission"].sum(), df["realizedPNL"].sum()
    

                        