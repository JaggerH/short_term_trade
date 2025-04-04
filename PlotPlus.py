import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import matplotlib.ticker as mtick

from utils import macd, vwap

class PlotPlus:
    def __init__(self, df):
        self.df = df
        
    def prepare_history(self, history):
        if len(history) == 0: return False
        if isinstance(history, list) and len(history) != 0:
            histories = pd.DataFrame(history)
        
        histories = histories.groupby(['date', 'direction'], as_index=False).agg({
            'date': 'first',  # 假设 symbol 不变，取第一行的值
            'symbol': 'first',    # 同理，取第一个
            'strategy': 'first',  # 同理，取第一个
            'open_or_close': 'first',   # 同理，取第一个
            'direction': 'first',   # 同理，取第一个
            'price': 'first',   # 同理，取第一个
            'amount': 'sum',     # 对 amount 求和
            'commission': 'sum',     # 对 commission 求和
            'pnl': 'sum'     # 对 pnl 求和
        })
        
        histories['date'] = pd.to_datetime(histories['date'])
        histories.set_index('date', inplace=True, drop=True)

        self.df['signal'] = histories['direction']
        self.df['amount'] = histories['amount']
        return True
    
    def plot_basic(self, lines=None, style_type="line"):
        """
        假设df只包含OHLC and date
        """
        assert style_type in ["line", "candle"], "style_type参数只接收line或candle"
        if not isinstance(self.df.index, pd.DatetimeIndex):
            self.df = self.df.set_index(pd.to_datetime(self.df['date']))
            
        macd_panel = self.generate_macd_panel()
        vwap_panel = self.generate_vwap_panel()
        ema_panel  = self.generate_ema_panel()
        volume_panel = self.generate_volume_panel()
        panels = macd_panel + vwap_panel + ema_panel + volume_panel
        
        if lines is not None:
            line_panel = []
            for line in lines:
                line_instance = mpf.make_addplot([line] * len(self.df.index), linestyle='--', panel=0)
                line_panel.append(line_instance)
            panels += line_panel
            
        fig, self.axes = mpf.plot(self.df, type=style_type, 
                style='yahoo',
                ylabel='Price', 
                addplot=panels,
                panel_ratios=(3, 1, 1),  # 设置主图和副图的比例
                volume=False,         # 不显示成交量
                figsize=(10, 6),
                returnfig=True)       # 返回figure和axes对象
        
        self.generate_pct_change()
    
    def generate_volume_panel(self):
        # 根据涨跌设置颜色，涨(收盘价>=开盘价)为红色，跌为绿色
        colors = np.where(self.df['close'] >= self.df['open'], 'r', 'g')

        # 创建成交量的 addplot
        volume_panel = mpf.make_addplot(self.df['volume'], panel=2, type='bar', color=colors, ylabel='Volume')
        return [volume_panel]

    def generate_ema_panel(self):
        if 'cv_10' not in self.df.columns:
            self.df['cv_10'] = self.df['close'].ewm(span=10, adjust=False).mean()
        
        ema_panel = [
            mpf.make_addplot(self.df['cv_10'], color='green', linestyle='solid', width=1, label='EMA 10')
        ]
        
        return ema_panel
    
    def generate_macd_panel(self, panel_id=1):
        if 'DIF' not in self.df.columns:
            self.df['DIF'], self.df['DEA'], self.df['MACD'] = macd(self.df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        
        macd_panel = [
                    mpf.make_addplot(self.df['DIF'], panel=panel_id, color='b', alpha=0.5),
                    mpf.make_addplot(self.df['DEA'], panel=panel_id, color='r', alpha=0.5),
                    mpf.make_addplot(self.df['MACD'], panel=panel_id, color='grey', type='bar', width=0.7, alpha=0.5)
                ]
        
        return macd_panel
    
    def generate_vwap_panel(self):
        if 'vwap' not in self.df.columns:
            self.df['vwap'] = vwap(self.df['close'], self.df['volume'])
        
        vwap_panel = [
            mpf.make_addplot(self.df['vwap'], color='orange', linestyle='solid', width=1.2, label='Average (VWAP)')
        ]
        
        return vwap_panel

    def mark_point(self, df, y_column, color='blue', marker='o', label='Pivot', text_offset=0.1):
        """
        在指定的 Matplotlib 轴上标记数据点。
        
        参数:
        - ax: Matplotlib 的 Axes 对象
        - df: 包含拐点数据的 DataFrame
        - y_column: y 轴数据列名（如 'ema_10'）
        - color: 标记点的颜色，默认蓝色
        - marker: 标记点的形状，默认 'o'
        - label: 图例标签，默认 'Pivot'
        - text_offset: 文字标注的偏移量，默认 0.1
        """
        if df.empty:
            return
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.set_index(pd.to_datetime(df['date']))
        # 获取标记点的 x 轴位置（索引）
        x_positions = [self.df.index.get_loc(idx) for idx in df.index]
        y_positions = df[y_column]
        ax_main = self.axes[0]  # 主图的Axes对象
        # 在图上标记拐点
        ax_main.scatter(x_positions, y_positions, label=label, color=color, marker=marker, s=10)

        # 标注拐点上的文本信息（默认显示斜率）
        # for x, y in zip(x_positions, y_positions):
        #     print(x, y)
        #     ax_main.text(x, y + text_offset, f"{df.loc[x, 'slope']:.4f}", 
        #             color=color, fontsize=8, ha='center', va='bottom')
        
    def mark_bs_point(self, history):
        """
        在主图上标记买卖点
        """
        if not self.prepare_history(history): return
        ax_main = self.axes[0]  # 主图的Axes对象
        
        # 提取买卖点的索引和价格
        buy_signals = self.df[self.df['signal'] == 'BUY']
        buy_signals_position = [self.df.index.get_loc(idx) for idx in buy_signals.index]
        # 在主图上标注买卖点
        if buy_signals_position:
            ax_main.scatter(buy_signals_position, buy_signals['close'], label='Buy', color='green', marker='^', s=20)
            
            for idx, pos in zip(buy_signals.index, buy_signals_position):
                quantity = buy_signals.loc[idx, 'amount']  # 假设数量列是 'quantity'
                ax_main.text(pos, buy_signals.loc[idx, 'close'] + 0.1,  # 在价格上方添加文本，+0.1调整位置
                            str(quantity),  # 作为文本标注数量
                            color='green', fontsize=8, ha='center', va='bottom')
            
        sell_signals = self.df[self.df['signal'] == 'SELL']
        sell_signals_position = [self.df.index.get_loc(idx) for idx in sell_signals.index]
        if sell_signals_position:
            ax_main.scatter(sell_signals_position, sell_signals['close'], label='Sell', color='red', marker='v', s=20)
            
            # 标注卖出数量
            for idx, pos in zip(sell_signals.index, sell_signals_position):
                quantity = sell_signals.loc[idx, 'amount']  # 假设数量列是 'quantity'
                ax_main.text(pos, sell_signals.loc[idx, 'close'] - 0.1,  # 在价格下方添加文本，-0.1调整位置
                            str(quantity),  # 作为文本标注数量
                            color='red', fontsize=8, ha='center', va='top')
    
    def mark_segment(self, column, color="gray"):
        periods = []
        start = None

        for i in range(len(self.df)):
            if self.df.iloc[i][column]:
                if start is None:
                    start = self.df.index[i]  # 记录震荡市场的起点
            else:
                if start is not None:
                    periods.append((start, self.df.index[i-1]))  # 记录起止时间
                    start = None

        if start is not None:
            periods.append((start, self.df.index[-1]))
            
        for start, end in periods:
            start_idx = self.df.index.get_loc(start)
            end_idx = self.df.index.get_loc(end)
            self.axes[0].axvspan(start_idx, end_idx, color=color, alpha=0.3)  # 震荡区域填充背景色
        
    def generate_pct_change(self):
        self.df['return'] = (self.df['close'] / self.df.iloc[0]['close'] - 1) * 100

        # 创建右侧轴
        ax_return = self.axes[0].twinx()

        # 设置右侧轴的刻度
        ax_return.set_ylabel('Return (%)', color='blue')
        ax_return.set_yticks(np.linspace(self.df['return'].min(), self.df['return'].max(), 6))  # 设定6个刻度
        ax_return.yaxis.set_major_formatter(mtick.PercentFormatter())  # 显示百分比格式
        ax_return.tick_params(axis='y', labelcolor='blue')

        # 让右侧 y 轴的范围与主 y 轴对齐
        ax_return.set_ylim(self.df['return'].min(), self.df['return'].max())
    
    def show(self):
        plt.show()