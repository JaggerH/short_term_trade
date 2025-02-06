from ib_insync import *
import time
from datetime import datetime, timedelta
from PositionManager import PositionManager
from Structure import Structure
import pandas as pd

# 连接到 IBKR
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# # 定义标的列表
symbols = [('TSLA', 'NASDAQ'), ('SOXL', 'ARCA'), ('NVDA', 'NASDAQ'), ('PLTR', 'NASDAQ'), ('AVGO', 'NASDAQ')]  # 替换为你的标的
contracts = [Stock(symbol, 'SMART', 'USD', primaryExchange=exchange) for symbol, exchange in symbols]
        
pm = PositionManager(ib, debug=False)

# 定义获取行情的函数
def fetch_minute_data(contract):
    end_time = ''
    duration = '1 D'
    bar_size = '1 min'
    try_count = 3
    while True:
        data = ib.reqHistoricalData(
            contract,
            endDateTime=end_time,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=True
        )
        if not bars:
            print(contract.symbol, ' bars is empty')
            try_count += 1
            time.sleep(1)
            if try_count >= 4:
                raise "尝试3次数据获取均为空"
        else:
            return data

# 等待到下一个完整的 01 秒
def wait_for_next_minute():
    now = datetime.now()
    # 计算下一个 01 秒的时间点
    next_minute = (now.replace(second=1, microsecond=0) + 
                   timedelta(minutes=1) if now.second >= 1 else now.replace(second=1, microsecond=0))
    sleep_time = (next_minute - now).total_seconds()
    time.sleep(sleep_time)

# 主循环
try:
    while True:
        # 等待到每分钟的 01 秒
        wait_for_next_minute()
        # 获取数据
        for contract in contracts:
            bars = fetch_minute_data(contract)

            bars = pd.DataFrame(bars)
            structure = Structure()
            current_time = bars.iloc[-1]['date']
            pm.update(contract, structure, bars, current_time)
except KeyboardInterrupt:
    print("程序已停止")
finally:
    # 断开连接
    ib.disconnect()
