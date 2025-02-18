from ib_insync import *
import pandas as pd
import yaml
from functools import partial
from PositionManagerPlus import PositionManager
from Structure import Structure

# if __name__ == "__main__":
#     trade_app = TradeApp(debug=False)
#     trade_app.subscribe_to_bars()
    
class TradeApp:
    def __init__(self, config_file="config.yml", debug=False, port=7497, clientId=1):
        # 连接到 IBKR
        self.ib = IB()
        self.ib.connect('127.0.0.1', port, clientId=clientId)

        # 加载配置文件
        with open(config_file, "r", encoding="utf-8") as file:
            symbols = yaml.safe_load(file)["symbols"]
        
        # 创建合约列表
        self.contracts = [Stock(symbol, 'SMART', 'USD', primaryExchange=exchange) for symbol, exchange in symbols]
        
        # 初始化 PositionManager
        self.pm = PositionManager(self.ib, debug=debug, config_file=config_file)
    
    def on_bar_update(self, contract, bars, has_new_bar):
        if has_new_bar:
            bars = pd.DataFrame(bars)
            structure = Structure()
            current_time = bars.iloc[-1]['date']
            self.pm.update(contract, structure, bars, current_time)

    def subscribe_to_bars(self):
        # 遍历合约并订阅行情
        try:
            for contract in self.contracts:
                # 请求历史数据并订阅实时更新
                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr='1 D',  # 请求1天的数据
                    barSizeSetting='1 min',  # 设置时间周期为1分钟
                    whatToShow='TRADES',  # 显示交易数据
                    useRTH=True,  # 仅使用常规交易时间
                    keepUpToDate=True  # 保持订阅最新数据
                )
                
                # 使用 functools.partial 将 contract 与回调函数绑定
                bars.updateEvent += partial(self.on_bar_update, contract)
            
            # 保持脚本运行，等待数据更新
            self.ib.run()
        except KeyboardInterrupt:
            print("程序已停止")
        finally:
            self.pm.save()
            self.ib.disconnect()
            