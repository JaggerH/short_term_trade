import pandas as pd
import time

# 读取CSV文件并加载数据
def load_csv_data(file_path):
    df = pd.read_csv(file_path, parse_dates=['date'])
    return df

# 模拟的等待函数，每2秒触发一次
def fake_wait_for_next_minute():
    time.sleep(0.01)
    
def fake_fetch_minute_data(contract, csv_data, current_index):
    if current_index < len(csv_data):
        bars = csv_data.loc[:current_index].to_dict(orient='records')
        return bars, current_index + 1
    else:
        return (None, None)