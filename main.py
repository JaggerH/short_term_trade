from ib_insync import *
import time
from datetime import datetime, timedelta
from PositionManager import PositionManager
from Structure import Structure

# 连接到 IBKR
# ib = IB()
# ib.connect('127.0.0.1', 7496, clientId=1)

# # 定义标的列表
symbols = [('TSLA', 'NASDAQ'), ('SOXL', 'ARCA'), ('NVDA', 'NASDAQ')]  # 替换为你的标的
contracts = [Stock(symbol, 'SMART', 'USD', primaryExchange=exchange) for symbol, exchange in symbols]
# for contract in contracts:
#     contract_details = ib.reqContractDetails(contract)
#     if not contract_details:
#         print("未找到合约，请检查合约定义")
#         ib.disconnect()
#         exit()
        
pm = PositionManager(True)

# 定义获取行情的函数
def fetch_minute_data(contract):
    end_time = ''
    duration = '1 D'
    bar_size = '1 min'
    data = ib.reqHistoricalData(
        contract,
        endDateTime=end_time,
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow='TRADES',
        useRTH=True
    )
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
# try:
#     # 加载CSV数据
#     csv_file_path = 'historical_data.csv'  # 替换为你的CSV文件路径
#     from fake_data import *
#     csv_data = load_csv_data(csv_file_path)

#     current_index = 0
#     while True:
#         # 等待到每分钟的 01 秒
#         wait_for_next_minute()
#         # 获取数据
#         for contract in contracts:
#             bars = fetch_minute_data(contract)
#             if not bars:
#                 print(contract.symbol, ' bars is empty')
#             # print(f"Symbol: {contract.symbol}")
#             structure = Structure()
#             current_time = datetime.now()
#             pm.update(structure, bars, current_time)

# except KeyboardInterrupt:
#     print("程序已停止")
# finally:
#     # 断开连接
#     ib.disconnect()
contracts = contracts[:1]

try:
    # 加载CSV数据
    csv_file_path = 'historical_data.csv'  # 替换为你的CSV文件路径
    from fake_data import *
    csv_data = load_csv_data(csv_file_path)

    current_index = 0
    while True:
        # 等待到每分钟的 01 秒
        fake_wait_for_next_minute()
        # 获取数据
        for contract in contracts:
            bars, current_index = fake_fetch_minute_data(contract, csv_data, current_index)
            # print(f"New data for {contract}: {bars}")
            bars = pd.DataFrame(bars)
            # print(df)
            # print(f"Symbol: {contract.symbol}")
            structure = Structure()
            # structure.cal(bars)
            current_time = bars.iloc[-1]["date"]
            pm.update(structure, bars, current_time)

except KeyboardInterrupt:
    print("程序已停止")
# finally:
#     # 断开连接
#     ib.disconnect()
