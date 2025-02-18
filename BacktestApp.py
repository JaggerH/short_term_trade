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

class BacktestApp(TradeApp):  # 继承自 TradeApp 以便复用已有代码
    def __init__(self, config_file="config.yml", **kwargs):
        super().__init__(config_file=config_file, **kwargs)
        # 获取 Redis 客户端
        self.redis_client = self.get_redis(config_file)

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
        
        if cached_data:
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

    def data_iterator(self, start_date, end_date, durationStr='1 D', barSizeSetting='1 min'):
        # 遍历日期范围
        date_range = pd.date_range(start=start_date, end=end_date)
        for date in date_range:
            # 遍历每个合约
            for contract in self.contracts:
                # 获取历史数据
                bars_df = self.get_historical_data(contract, date.strftime('%Y-%m-%d'), durationStr, barSizeSetting)
                yield contract, date, bars_df  # 返回每次遍历的合约、日期和数据
                
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
            
    def statistic(self):
        """
        计算交易日志的区间累计收益和最大回撤。

        返回:
            dict: {'cumulative_pnl': float, 'max_drawdown': float}
        """
        # 获取交易日志
        df = pd.DataFrame(self.pm.trade_log)

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
        daily_pnl["drawdown"] = (daily_pnl["cumulative_pnl"] - daily_pnl["rolling_max"]) / daily_pnl["rolling_max"]
        max_drawdown = daily_pnl["drawdown"].min()

        return {
            "cumulative_pnl": daily_pnl["cumulative_pnl"].iloc[-1],  # 最终累计收益
            "max_drawdown": max_drawdown if pd.notna(max_drawdown) else 0  # 避免 NaN
        }
        
    def plot_pnl(self):
        """
        绘制累计盈亏曲线。
        """
        # 初始化 DataFrame
        df = pd.DataFrame(self.pm.trade_log)

        if df.empty or "pnl" not in df.columns:
            print("交易日志为空，无法绘制盈亏曲线。")
            return

        # 确保 date 是 datetime 类型，并去掉时间部分（按天计算）
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 填充 NaN pnl 为 0（开仓时没有 pnl）
        df["pnl"] = df["pnl"].fillna(0)

        # 按日期求和，计算每日净收益
        daily_pnl = df.groupby("date")["pnl"].sum().reset_index()

        # 计算累计收益
        daily_pnl["cumulative_pnl"] = daily_pnl["pnl"].cumsum()

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