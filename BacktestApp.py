import sys
# 如果是在 Jupyter 环境中，应用 nest_asyncio
if 'ipykernel' in sys.modules:
    import nest_asyncio
    nest_asyncio.apply()
    
from TradeApp import TradeApp
from ib_insync import *
import pandas as pd
import yaml
import redis
import pytz
import matplotlib.pyplot as plt

# 解决get_historical_data中pd.read_json需要用
from io import StringIO
from utils import get_market_close_time

from PositionManagerPlus import PositionManager

class BacktestApp(TradeApp):  # 继承自 TradeApp 以便复用已有代码
    def __init__(self, config_file="config.yml", **kwargs):
        super().__init__(config_file=config_file, **kwargs)
        debug = kwargs.get('debug', False)  # 默认值 False
        self.redis_client = self.get_redis(config_file)
        self.pm = PositionManager(None, self.__class__.__name__, debug=debug, config_file=config_file)

    def get_redis(self, config_file):
        if not hasattr(self, '_redis'):
            # 加载配置
            with open(config_file, "r") as file:
                config = yaml.safe_load(file)

            redis_config = config.get("redis", {})
            self._redis = redis.Redis(**redis_config)
        return self._redis

    def get_historical_data(self, contract, date, durationStr='1 D', barSizeSetting='1 min'):
        redis_key = f"{contract.symbol}_{date}_{durationStr}_{barSizeSetting}"
        cached_data = self.redis_client.get(redis_key)
        
        if cached_data is not None:
            cached_data_str = cached_data.decode('utf-8')
            bars_df = pd.read_json(StringIO(cached_data_str))
            
            # 如果 barSizeSetting 是 '1 day'，修改 date 格式为 datetime.date
            if barSizeSetting.endswith('1 day'):
                bars_df['date'] = pd.to_datetime(bars_df['date']).dt.date
            # 如果是分钟线数据，进行时区转换
            elif barSizeSetting.endswith('min'):
                eastern = pytz.timezone('US/Eastern')
                bars_df['date'] = pd.to_datetime(bars_df['date']).dt.tz_localize('UTC').dt.tz_convert(eastern)
                
            return bars_df

        # 如果缓存中没有数据，则请求 IBKR 数据
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=date,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )

        # 将数据转换为 DataFrame
        bars_df = pd.DataFrame(bars)
        if len(bars_df) > 0:
            # 将 DataFrame 转换为 JSON 格式并缓存到 Redis
            self.redis_client.set(redis_key, bars_df.to_json(orient='records'))
        return bars_df

    def backtest(self, start_date, end_date, durationStr='1 D', barSizeSetting='1 min'):
        # 假设回测数据需要处理多个日期
        date_range = pd.date_range(start=start_date, end=end_date)
        for date in date_range:
            for contract in self.contracts:
                # 获取历史数据
                bars_df = self.get_historical_data(contract, date.strftime('%Y-%m-%d'), durationStr, barSizeSetting)
                # 这里可以加入回测的逻辑处理，例如更新仓位、计算指标等
                print(f"处理 {contract.symbol} 在 {date.strftime('%Y-%m-%d')} 的数据")

    def daily_unorder_iterator(self, end_date, durationStr='100 D'):
        """
            -------- 日内无序运算 ----------
            
            回测一段交易时间（多日）内 多个股票在同一策略内运行的情况
            此处先以StrcutreReserve为开始
            即 交易只在日内存在先后顺序 每个合约在单个交易日的策略都是独立的
            即便如此也需要在策略内对find_position做限定 将find_position的date限制在同一天
            
            目前的限制：
            2025.02.22
            没有对交易金额的上限进行限制 只要存在信号就可以开仓
            
            这样的优势在于可以进行多线程并发运算
        """
        # 先读取日期区间日K OHLC 然后返回minutes数据
        end_date = get_market_close_time(end_date)
        for contract in self.contracts:
            # daily即是日线数据
            daily = self.get_historical_data(contract, end_date, durationStr, '1 day')
            for index, row in daily.iterrows():
                today = get_market_close_time(row["date"])
                minutes = self.get_historical_data(contract, today)
                yield contract, today, minutes
                
    def minute_iterator(self, contract, date):
        """
        返回单个合约在指定日期的分钟线数据迭代器。
        """
        _date = get_market_close_time(date)
        bars_df = self.get_historical_data(contract, _date, '1 D', '1 min')
        intern_index = 0
        
        # 直接在迭代器中处理遍历逻辑
        while intern_index < len(bars_df):
            bars = bars_df.loc[:intern_index]
            intern_index += 1
            yield bars  # 使用 yield 返回当前的分钟数据
    
    def custom_iterator(self, contract, date, callback):
        _date = get_market_close_time(date)
        bars_df = self.get_historical_data(contract, _date, '1 D', '1 min')
        bars_df = callback(bars_df)
        intern_index = 0
        
        # 直接在迭代器中处理遍历逻辑
        while intern_index < len(bars_df):
            bars = bars_df.loc[:intern_index]
            intern_index += 1
            yield bars  # 使用 yield 返回当前的分钟数据
            
    def custom_iterator_minute_data(self, bars, callback):
        bars_df = callback(bars)
        intern_index = 0
        
        while intern_index < len(bars_df):
            bars = bars_df.loc[:intern_index]
            intern_index += 1
            yield bars  # 使用 yield 返回当前的分钟数据
            
    def statistic(self):
        """
        计算交易日志的区间累计收益和最大回撤。

        返回:
            dict: {'cumulative_pnl': float, 'max_drawdown': float}
        """
        # 获取交易日志
        df = pd.DataFrame(self.pm.trade_log)
        df["date"] = pd.to_datetime(df["date"], errors='coerce') 
        
        if df.empty or "pnl" not in df.columns:
            return {"cumulative_pnl": 0, "max_drawdown": 0}  # 交易日志为空，返回 0

        # 确保 date 是 datetime 类型，并去掉时间部分（按天计算）
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 填充 NaN pnl 为 0（开仓时没有 pnl）
        df["pnl"] = df["pnl"].fillna(0)

        # 按日期求和，计算每日净收益
        daily_pnl = df.groupby("date")["pnl"].sum().reset_index()

        # 计算累计收益
        daily_pnl["cumulative_pnl"] = daily_pnl["pnl"].cumsum()

        # 计算最大回撤
        daily_pnl["rolling_max"] = daily_pnl["cumulative_pnl"].cummax()
        daily_pnl["drawdown"] = (daily_pnl["rolling_max"] - daily_pnl["cumulative_pnl"]) / daily_pnl["rolling_max"]
        max_drawdown = daily_pnl["drawdown"].min()

        return {
            "cumulative_pnl": daily_pnl["cumulative_pnl"].iloc[-1],  # 最终累计收益
            "max_drawdown": max_drawdown if pd.notna(max_drawdown) else 0,  # 避免 NaN
            "commission": df["commission"].sum()
        }
        
    def plot_pnl(self):
        """
        绘制累计盈亏曲线。
        """
        # 初始化 DataFrame
        df = pd.DataFrame(self.pm.trade_log)
        df["date"] = pd.to_datetime(df["date"], errors='coerce') 
        
        if df.empty or "pnl" not in df.columns or "commission" not in df.columns:
            print("交易日志为空，或缺少 pnl/commission 列，无法绘制盈亏曲线。")
            return

        # 确保 date 是 datetime 类型，并去掉时间部分（按天计算）
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 填充 NaN pnl 为 0（开仓时没有 pnl）
        df["pnl"] = df["pnl"].fillna(0)
        df["commission"] = df["commission"].fillna(0)

        # 扣减佣金后的盈亏
        df["net_pnl"] = df["pnl"] - df["commission"]

        # 按日期求和，计算每日净收益
        daily_pnl = df.groupby("date")["net_pnl"].sum().reset_index()

        # 计算累计收益
        daily_pnl["cumulative_pnl"] = daily_pnl["net_pnl"].cumsum()

        # 绘制盈亏曲线
        plt.figure(figsize=(10, 6))
        plt.plot(daily_pnl["date"], daily_pnl["cumulative_pnl"], label="P&L", color='b', linewidth=2)
        plt.xlabel("Date")
        plt.ylabel("Profit and Loss")
        plt.title("PnL Curve")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()