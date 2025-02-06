from ib_insync import *

from PositionManager import PositionManager
from Structure import Structure
import pandas as pd

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=5)
pm = PositionManager(ib, False)

csv_file_paths = [
    "quotes/BILI_20250131.csv",
    "quotes/TSLA_20250107.csv",
    "quotes/TSLA_20250131.csv",
    "quotes/NVDA_20250203.csv",
    "quotes/SOXL_20250203.csv"
]

from fake_data import *
csv_data = load_csv_data(csv_file_paths[4])

current_index = 0
contract = Stock('SOXL', 'SMART', 'USD', primaryExchange='ARCA')
while True:
    bars, current_index = fake_fetch_minute_data(contract, csv_data, current_index)
    if bars is None: break
    bars = pd.DataFrame(bars)
    structure = Structure()
    # structure.cal(bars)
    current_time = bars.iloc[-1]["date"]
    pm.update(contract, structure, bars, current_time)
    fake_wait_for_next_minute()