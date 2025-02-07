from ib_insync import *
import time
from datetime import datetime, timedelta
from PositionManager import PositionManager
from Structure import Structure
import pandas as pd
import yaml
from functools import partial

# 连接到 IBKR
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# 定义标的列表
# symbols = [('TSLA', 'NASDAQ'), ('SOXL', 'ARCA'), ('NVDA', 'NASDAQ'), ('PLTR', 'NASDAQ'), ('AVGO', 'NASDAQ')]  # 替换为你的标的
with open("config.yml", "r", encoding="utf-8") as file:
    symbols = yaml.safe_load(file)["symbols"]
contracts = [Stock(symbol, 'SMART', 'USD', primaryExchange=exchange) for symbol, exchange in symbols]
        
pm = PositionManager(ib, debug=False)

# 定义获取行情的函数
# def fetch_minute_data(contract):
#     end_time = ''
#     duration = '1 D'
#     bar_size = '1 min'
#     try_count = 3
#     while True:
#         data = ib.reqHistoricalData(
#             contract,
#             endDateTime=end_time,
#             durationStr=duration,
#             barSizeSetting=bar_size,
#             whatToShow='TRADES',
#             useRTH=True
#         )
#         if not data:
#             print(contract.symbol, ' bars is empty')
#             try_count += 1
#             time.sleep(1)
#             if try_count >= 4:
#                 raise "尝试3次数据获取均为空"
#         else:
#             return data

# # 等待到下一个完整的 01 秒
# def wait_for_next_minute():
#     now = datetime.now()
#     # 计算下一个 01 秒的时间点
#     next_minute = (now.replace(second=1, microsecond=0) + 
#                    timedelta(minutes=1) if now.second >= 1 else now.replace(second=1, microsecond=0))
#     sleep_time = (next_minute - now).total_seconds()
#     time.sleep(sleep_time)

# # 主循环
# try:
#     while True:
#         # 等待到每分钟的 01 秒
#         wait_for_next_minute()
#         # 获取数据
#         for contract in contracts:
#             bars = fetch_minute_data(contract)
#             bars = pd.DataFrame(bars)
#             structure = Structure()
#             current_time = bars.iloc[-1]['date']
#             pm.update(contract, structure, bars, current_time)
# except KeyboardInterrupt:
#     print("程序已停止")
# finally:
#     # 断开连接
#     ib.disconnect()

def on_bar_update(contract, bars, has_new_bar):
    if has_new_bar:  # 检查是否有新的K线数据
        bars = pd.DataFrame(bars)
        # print(f"New bar for {contract.symbol}: {bars.iloc[0]['date'], bars.iloc[-1]['date']}")  # 打印最新K线数据
        structure = Structure()
        current_time = bars.iloc[-1]['date']
        pm.update(contract, structure, bars, current_time)
        
# 遍历合约并订阅行情
try:
    for contract in contracts:
        # 请求历史数据并订阅实时更新
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='1 D',  # 请求1天的数据
            barSizeSetting='1 min',  # 设置时间周期为1分钟
            whatToShow='TRADES',  # 显示交易数据
            useRTH=False,  # 仅使用常规交易时间
            keepUpToDate=True  # 保持订阅最新数据
        )
        
        # 使用 functools.partial 将 contract_name 与回调函数绑定
        bars.updateEvent += partial(on_bar_update, contract)

    # 保持脚本运行，等待数据更新
    ib.run()
except KeyboardInterrupt:
    print("程序已停止")
finally:
    # 断开连接
    ib.disconnect()