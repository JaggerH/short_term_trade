from ib_insync import *
import pandas as pd
import yaml
from functools import partial
from PositionManagerPlus import PositionManager
import time
from tqdm import tqdm # 进度条工具

class TradeApp:
    """
    e.g.
    import pandas as pd

    from TradeApp import TradeApp
    from StructureReserve import StructureReserve

    class StructureTradeApp(TradeApp):
        def on_bar_update(self, contract, bars, has_new_bar):
            if has_new_bar:
                bars = pd.DataFrame(bars)
                print('on_bar_update', contract.symbol, bars.iloc[-1]["date"], bars.iloc[-1]["close"])
                structure = StructureReserve()
                structure.update(contract, bars, ta.pm)

    if __name__ == "__main__":            
        ta = StructureTradeApp()
        ta.subscribe_to_bars()
    """
    def __init__(self, config_file="config.yml", debug=False, host="127.0.0.1", port=7497, clientId=1, **kwargs):
        self.ib = IB()
        self.host = host
        self.port = port
        self.clientId = clientId
        
        self.connected = False
        self.connect_to_ibkr()  # 尝试连接IBKR

        # 加载配置文件
        with open(config_file, "r", encoding="utf-8") as file:
            symbols = yaml.safe_load(file)["symbols"]
        
        # 创建合约列表
        self.contracts = [Stock(symbol, 'SMART', 'USD', primaryExchange=exchange) for symbol, exchange in symbols]
        
        # 初始化 PositionManager
        self.pm = PositionManager(self.ib, self.__class__.__name__, debug=debug, config_file=config_file)
    
    def connect_to_ibkr(self):
        """
        尝试连接到IBKR，如果连接失败则自动重试。
        """
        try:
            self.ib.connect(self.host, self.port, clientId=self.clientId)
            self.connected = True
            # print(f"成功连接到IBKR（{self.clientId}）")
        except Exception as e:
            print(f"连接IBKR失败: {e}")
            self.reconnect()

    def reconnect(self):
        """
        自动重连的逻辑：连接失败后，间隔一定时间自动重试。
        """
        retry_interval = 5  # 初始重试间隔（秒）
        while not self.connected:
            print(f"等待 {retry_interval} 秒后重试连接...")
            time.sleep(retry_interval)
            try:
                self.ib.connect(self.host, self.port, clientId=self.clientId)
                self.connected = True
                print(f"成功连接到IBKR（{self.clientId}）")
            except Exception as e:
                print(f"连接失败: {e}")
                retry_interval = min(retry_interval * 2, 60)  # 每次重试间隔逐渐增加（最多60秒）

    def on_bar_update(self, contract, bars, has_new_bar):
        # 该函数仅占位，尚未实现
        raise NotImplementedError("on_bar_update方法尚未实现")

    def subscribe_to_bars(self):
        try:
            for contract in tqdm(self.contracts, desc="合约行情订阅", unit="contract"):
                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr='1 D',  # 请求1天的数据
                    barSizeSetting='1 min',  # 设置时间周期为1分钟
                    whatToShow='TRADES',  # 显示交易数据
                    useRTH=True,  # 仅使用常规交易时间
                    keepUpToDate=True  # 保持订阅最新数据
                )
                
                bars.updateEvent += partial(self.on_bar_update, contract)
            print('Start Subcribe!')
            # 保持脚本运行，等待数据更新
            self.ib.run()

        except KeyboardInterrupt:
            print("程序已停止")
        except Exception as e:
            print(f"出现错误: {e}")
            if not self.connected:
                print("尝试重新连接...")
                self.reconnect()
                self.subscribe_to_bars()  # 重新订阅行情
        finally:
            self.pm.save()
            self.ib.disconnect()

# if __name__ == "__main__":
#     trade_app = TradeApp(debug=False)
#     trade_app.subscribe_to_bars()
