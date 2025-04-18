import sys
# 如果是在 Jupyter 环境中，应用 nest_asyncio
if 'ipykernel' in sys.modules:
    import nest_asyncio
    nest_asyncio.apply()
    
from TradeApp import TradeApp
from ib_insync import *
import pandas as pd
import numpy as np
import yaml
import redis
import pytz
import matplotlib.pyplot as plt
import os
import zipfile
from datetime import datetime, timedelta

# 解决get_historical_data中pd.read_json需要用
from io import StringIO
from utils import get_market_close_time

from PositionManagerPlus import PositionManager

class BacktestApp(TradeApp):  # 继承自 TradeApp 以便复用已有代码
    def __init__(self, config_file="config.yml", autoConnect=False, **kwargs):
        super().__init__(config_file=config_file, autoConnect=autoConnect, **kwargs)
        debug = kwargs.get('debug', False)  # 默认值 False
        self.redis_client = self.get_redis(config_file)
        
        self.pm = PositionManager(None, self.__class__.__name__, debug=debug, config_file=config_file)
        self.last_price = {}
        self.initial_capital = self.pm.net_liquidation
        self.onBarUpdateEvent = [self.update_position_manager_net_liquidation, self.on_bar_update]
        self.afterMarketCloseEvent = [self._after_market_close]
        self.daily_net_liquidation = []
        
        with open(config_file, "r", encoding="utf-8") as file:
            self.offline_tick_root = yaml.safe_load(file)["offline_ticks_path"]

    def get_redis(self, config_file):
        if not hasattr(self, '_redis'):
            # 加载配置
            with open(config_file, "r") as file:
                config = yaml.safe_load(file)

            redis_config = config.get("redis", {})
            self._redis = redis.Redis(**redis_config)
        return self._redis

    def get_historical_data(self, contract, date, durationStr='1 D', barSizeSetting='1 min'):
        date = get_market_close_time(date)
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

    def read_offline_tick(self, contract, date):
        """
        根据合约和日期，从离线数据缓存中只解压指定合约的 tick CSV 文件，并通过 pandas 读取。
        
        参数：
        contract: 合约对象，要求有 .symbol 属性（例如 "AAPL"）
        date: 字符串格式的日期，格式为 "YYYYMMDD"，例如 "20250123"
        data_root: 离线数据根目录，例如 "/数据根目录"
        
        返回：
        包含 tick 数据的 pandas DataFrame
        """
        date = str(date).replace("-", "")[:10]
        # 构造月份目录，例如 "202501"
        month_folder = date[:6]
        # 构造 zip 文件的完整路径，例如 /数据根目录/202501/20250123.zip
        zip_filename = f"{date}.zip"
        zip_path = os.path.join(self.offline_tick_root, month_folder, zip_filename)
        
        # 构造合约对应的 CSV 文件名称，假设命名格式为 {合约代码}.csv
        csv_filename = f"{contract.symbol}.csv"
        
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"找不到压缩文件: {zip_path}")
        
        # 打开 zip 压缩包，只读取指定的 CSV 文件
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # 检查压缩包内是否存在目标 CSV 文件
            if csv_filename not in zf.namelist():
                raise FileNotFoundError(f"在 {zip_path} 中未找到文件: {csv_filename}")
            # 使用 zf.open 打开目标 CSV 文件对象（无需完全解压整个包）
            with zf.open(csv_filename) as csv_file:
                # 直接使用 pandas 读取 CSV 文件数据
                df = pd.read_csv(csv_file)
                # datetime例子：'2025-03-21 04:00:00:000611'
                # 先替换最后一个冒号为点，变成 pandas 可识别格式
                df['datetime'] = df['datetime'].str.replace(r':(\d{6})$', r'.\1', regex=True)
                # 转换成时间类型
                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S.%f')
                # 补上美东时区（Eastern Time）
                eastern = pytz.timezone('America/New_York')
                df['datetime'] = df['datetime'].dt.tz_localize(eastern)
                df = df.rename(columns={'datetime': 'time'})
                df = df[(df['time'].dt.time >= pd.to_datetime('09:30').time()) &
                        (df['time'].dt.time <= pd.to_datetime('16:00').time())]
        return df
    
    def get_historical_ticks(self, contract, date):
        """
        连续获取指定交易日内所有 tick 数据。
        采用向后分页的策略：从交易结束时间开始，
        每次调用 reqHistoricalTicks(endDateTime=current_end) 获取最多 1000 条数据，
        并更新 current_end 为该批数据中的最早 tick 时间，
        循环直至覆盖到交易开始时间。
        """
        eastern = pytz.timezone('US/Eastern')
        # 定义交易开始和结束时间（美东时间）
        trading_start = eastern.localize(datetime.strptime(f"{date} 09:30:00", "%Y-%m-%d %H:%M:%S"))
        # trading_end = eastern.localize(datetime.strptime(f"{date} 16:00:00", "%Y-%m-%d %H:%M:%S"))
        trading_end = get_market_close_time(date)

        all_ticks = []
        # 从交易结束时间开始分页
        current_end = trading_end

        while current_end > trading_start:
            # 调用 reqHistoricalTicks：只传 endDateTime（startDateTime 留空）
            ticks = self.ib.reqHistoricalTicks(
                contract,
                "",  # startDateTime 为空
                current_end,
                1000,
                whatToShow='TRADES',
                useRth=True,
                ignoreSize=False
            )

            # 将返回的 tick 数据转换为 DataFrame
            partial_df = pd.DataFrame([{
                'time': t.time.astimezone(eastern),
                'price': t.price,
                'size': t.size
            } for t in ticks])
            print(partial_df.iloc[0]["time"])
            if partial_df.empty:
                break

            all_ticks.append(partial_df)
            
            # 本批数据通常按降序排列，获取最早的 tick 时间
            oldest_tick_time = partial_df['time'].min()

            # 如果已经获取到的最早时间早于或等于交易开始，则退出循环
            if oldest_tick_time <= trading_start:
                break

            # 更新 current_end 为最早 tick 的时间（下一次请求将获取更早的数据）
            current_end = oldest_tick_time

        if all_ticks:
            # 合并所有分页数据，并按时间升序排序
            ticks_df = pd.concat(all_ticks).reset_index(drop=True)
            ticks_df.sort_values(by='time', inplace=True)
            # 保留交易日内的数据
            ticks_df = ticks_df[ticks_df['time'] >= trading_start]
            return ticks_df
        else:
            return pd.DataFrame()
    
    def minutes_backtest(self, end_date, durationStr='100 D', pre_process_bar_callback=None):
        daily = self.get_historical_data(self.contracts[0], end_date, durationStr, '1 day')
        minutes = {}
        for index, row in daily.iterrows():
            for contract in self.contracts:
                today = get_market_close_time(row["date"])
                minutes[contract.symbol] = self.get_historical_data(contract, today) # 默认barSize 1 min
                if pre_process_bar_callback:
                    minutes[contract.symbol] = pre_process_bar_callback(minutes[contract.symbol])
                    
            for index in range(1, 391): # 分钟线长度390，range 391刚好到390
                for contract in self.contracts:
                    bars = minutes[contract.symbol][:index]
                    # self.on_bar_update(contract, bars, True)
                    # self.update_position_manager_net_liquidation(contract, bars)
                    for callback in self.onBarUpdateEvent:
                        callback(contract, bars, True)
                    
            for callback in self.afterMarketCloseEvent:
                callback(today)
                
    def _after_market_close(self, date):
        self.daily_net_liquidation.append({
            "date": date,
            "net_liquidation": self.pm.net_liquidation
        })
        
    def update_position_manager_net_liquidation(self, contract, bars, has_new_bar):
        if len(self.pm.positions) == 0 or not self.pm.debug: return # 测试情况下且position不为空才更新
        self.last_price[contract.symbol] = bars.iloc[-1]['close']
        df = pd.DataFrame(self.pm.positions).set_index('contract', drop=False)
        df['last_price'] = df['contract'].map(lambda contract: self.last_price.get(str(contract.symbol), None))
        market_value = (df['last_price'] * df['amount']).abs().sum()
        self.pm.net_liquidation = market_value + self.pm.available_funds
    
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

    def statistic(self, risk_free_rate=0.035):
        """
        计算交易日志的区间累计收益、最大回撤、波动率、夏普比率和每日超额收益。

        参数:
            risk_free_rate (float): 无风险利率，默认为0。

        返回:
            dict: {'cumulative_pnl': float, 'max_drawdown': float, 'sharpe_ratio': float, 'volatility': float, 'daily_return': float, 'commission': float}
        """
        # 如果没有每日净资产数据，返回零
        if not self.daily_net_liquidation:
            return {
                "cumulative_pnl": 0,
                "max_drawdown": 0,
                "volatility": 0,
                "sharpe_ratio": 0,
                "daily_return": 0,
                "commission": 0
            }

        # 将 daily_net_liquidation 转换为 DataFrame
        df = pd.DataFrame(self.daily_net_liquidation)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 计算每日收益率
        df["daily_return"] = df["net_liquidation"].pct_change()

        # 计算累计收益（这里直接使用最后的 net_liquidation 减去初始资本）
        cumulative_pnl = df["net_liquidation"].iloc[-1] - self.initial_capital

        # 计算最大回撤
        df["rolling_max"] = df["net_liquidation"].cummax()
        df["drawdown"] = (df["rolling_max"] - df["net_liquidation"]) / df["rolling_max"]
        max_drawdown = df["drawdown"].max()  # 最大回撤

        # 计算波动率（每日收益率的标准差）
        daily_volatility = df["daily_return"].std() * np.sqrt(252)  # 年化波动率

        # 计算平均每日超额收益
        avg_daily_return = df["daily_return"].mean()

        # 计算年化夏普比率
        if daily_volatility == 0:
            sharpe_ratio = None  # 如果波动率为0，夏普比率无法计算
        else:
            annualized_return = avg_daily_return * 252  # 假设252个交易日
            sharpe_ratio = (annualized_return - risk_free_rate) / daily_volatility

        return {
            "cumulative_pnl": cumulative_pnl,  # 最终累计收益
            "max_drawdown": max_drawdown if pd.notna(max_drawdown) else 0,  # 最大回撤
            "sharpe_ratio": sharpe_ratio,  # 夏普比率
            "volatility": daily_volatility,  # 波动率
            "daily_return": avg_daily_return, # 平均每日超额收益
            "commission": pd.DataFrame(self.pm.trade_log)["commission"].sum()
        }
        
    def plot_pnl(self):
        """
        绘制累计盈亏曲线，基于初始资金进行计算。
        """
        # 初始化 DataFrame
        df = pd.DataFrame(self.daily_net_liquidation)

        # 绘制盈亏曲线
        plt.figure(figsize=(10, 6))
        plt.plot(df["date"], df["net_liquidation"], label="P&L (Capital)", color='b', linewidth=2)
        plt.xlabel("Date")
        plt.ylabel("Capital (Profit and Loss)")
        plt.title("PnL Curve (Capital Based)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()
        