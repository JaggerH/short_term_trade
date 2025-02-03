import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import talib

from utils import cal_macd

def plot_trade_history(df, trade_history):
    """
    :params
    df: quote of stock
    trade_history: trade signals
    """
    # 确保 trade_history 不为空，并处理可能的空情况
    if trade_history and isinstance(trade_history, list):
        histories = pd.DataFrame(trade_history)
    else:
        histories = pd.DataFrame(columns=['date', 'direction'])

    # 确保 'date' 列存在并设置为索引
    if 'date' in histories.columns:
        histories['date'] = pd.to_datetime(histories['date'])
        histories.set_index('date', inplace=True, drop=False)
    else:
        # 如果没有 date 列，初始化一个空的索引
        histories = pd.DataFrame(columns=['date', 'direction'])
        histories.set_index('date', inplace=True, drop=False)
    
    # 将信号合并到主数据框，如果 histories 为空，'signal' 列会被填充为空值
    df['signal'] = histories['direction'] if not histories.empty else None

    # 提取买卖点的索引和价格
    buy_signals = df[df['signal'] == 'Bought']
    sell_signals = df[df['signal'] == 'Sold']
    
    buy_signals_position = [df.index.get_loc(idx) for idx in buy_signals.index]
    sell_signals_position = [df.index.get_loc(idx) for idx in sell_signals.index]
    
    if 'DIF' not in df.columns:
        df['DIF'], df['DEA'], df['MACD'] = cal_macd(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)

    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=[
                mpf.make_addplot(df['DIF'], panel=1, color='b', alpha=0.5),
                mpf.make_addplot(df['DEA'], panel=1, color='r', alpha=0.5),
                mpf.make_addplot(df['MACD'], panel=1, color='grey', type='bar', width=0.7, alpha=0.5)
            ],
            panel_ratios=(3, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象


    # 在主图上标注买卖点
    ax_main = axes[0]  # 主图的Axes对象
    if buy_signals_position:
        ax_main.scatter(buy_signals_position, buy_signals['close'], label='Buy', color='green', marker='^', s=20)
    if sell_signals_position:
        ax_main.scatter(sell_signals_position, sell_signals['close'], label='Sell', color='red', marker='v', s=20)

    macd_range = max(abs(df['DIF'].min()), abs(df['DIF'].max())) * 1.2
    macd_ylim = (-1 * macd_range, macd_range)
    for panel_id in range(1, 4):
        axes[panel_id].set_ylim(macd_ylim)

    # 显示图像
    plt.show()
    
def plot_debug_structure(df, trade_history):
    """
    debug structure
    :params
    df: quote of stock
    trade_history: trade signals
    """
    # 确保 trade_history 不为空，并处理可能的空情况
    if trade_history and isinstance(trade_history, list):
        histories = pd.DataFrame(trade_history)
    else:
        histories = pd.DataFrame(columns=['date', 'direction'])

    # 确保 'date' 列存在并设置为索引
    if 'date' in histories.columns:
        histories['date'] = pd.to_datetime(histories['date'])
        histories.set_index('date', inplace=True, drop=False)
    else:
        # 如果没有 date 列，初始化一个空的索引
        histories = pd.DataFrame(columns=['date', 'direction'])
        histories.set_index('date', inplace=True, drop=False)
    
    # 将信号合并到主数据框，如果 histories 为空，'signal' 列会被填充为空值
    df['signal'] = histories['direction'] if not histories.empty else None

    # 提取买卖点的索引和价格
    buy_signals = df[df['signal'] == 'Bought']
    sell_signals = df[df['signal'] == 'Sold']
    
    buy_signals_position = [df.index.get_loc(idx) for idx in buy_signals.index]
    sell_signals_position = [df.index.get_loc(idx) for idx in sell_signals.index]
    
    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=[
                mpf.make_addplot(df['DIF'], panel=1, color='b', alpha=0.5),
                mpf.make_addplot(df['DEA'], panel=1, color='r', alpha=0.5),
                mpf.make_addplot(df['MACD'], panel=1, color='grey', type='bar', width=0.7, alpha=0.5),
                mpf.make_addplot(df['DIF_scaled'], panel=2, color='b', alpha=0.5),
                mpf.make_addplot(df['DEA_scaled'], panel=2, color='r', alpha=0.5),
                mpf.make_addplot(df['MACD'], panel=2, color='grey', type='bar', width=0.7, alpha=0.5)
            ],
            panel_ratios=(3, 1, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象


    # 在主图上标注买卖点
    ax_main = axes[0]  # 主图的Axes对象
    if buy_signals_position:
        ax_main.scatter(buy_signals_position, buy_signals['close'], label='Buy', color='green', marker='^', s=20)
    if sell_signals_position:
        ax_main.scatter(sell_signals_position, sell_signals['close'], label='Sell', color='red', marker='v', s=20)

    macd_range = max(abs(df['DIF'].min()), abs(df['DIF'].max())) * 1.2
    macd_ylim = (-1 * macd_range, macd_range)
    for panel_id in range(1, 4):
        axes[panel_id].set_ylim(macd_ylim)

    # 显示图像
    plt.show()