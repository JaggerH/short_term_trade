import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import talib

from utils import macd, vwap

def prepare_trade_history(df, trade_history):
    # 确保 trade_history 不为空，并处理可能的空情况
    if isinstance(trade_history, list) and len(trade_history) != 0:
            histories = pd.DataFrame(trade_history)
    else:
        histories = pd.DataFrame(columns=['date', 'direction'])
    
    if len(histories) != 0:
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
    
    # 确保 'date' 列存在并设置为索引
    if 'date' in histories.columns:
        histories['date'] = pd.to_datetime(histories['date'])
        histories.set_index('date', inplace=True, drop=True)
    else:
        # 如果没有 date 列，初始化一个空的索引
        histories = pd.DataFrame(columns=['date', 'direction'])
        histories.set_index('date', inplace=True, drop=False)

    # 将信号合并到主数据框，如果 histories 为空，'signal' 列会被填充为空值
    df['signal'] = histories['direction'] if not histories.empty else None
    df['amount'] = histories['amount'] if not histories.empty else None
    return df

def mark_bs_point(df, axes):
    """
    在主图上标记买卖点
    """
    ax_main = axes[0]  # 主图的Axes对象
    
    # 提取买卖点的索引和价格
    buy_signals = df[df['signal'] == 'BUY']
    buy_signals_position = [df.index.get_loc(idx) for idx in buy_signals.index]
    # 在主图上标注买卖点
    if buy_signals_position:
        ax_main.scatter(buy_signals_position, buy_signals['close'], label='Buy', color='green', marker='^', s=20)
        
        for idx, pos in zip(buy_signals.index, buy_signals_position):
            quantity = buy_signals.loc[idx, 'amount']  # 假设数量列是 'quantity'
            ax_main.text(pos, buy_signals.loc[idx, 'close'] + 0.1,  # 在价格上方添加文本，+0.1调整位置
                         str(quantity),  # 作为文本标注数量
                         color='green', fontsize=8, ha='center', va='bottom')
        
    sell_signals = df[df['signal'] == 'SELL']
    sell_signals_position = [df.index.get_loc(idx) for idx in sell_signals.index]
    if sell_signals_position:
        ax_main.scatter(sell_signals_position, sell_signals['close'], label='Sell', color='red', marker='v', s=20)
        
        # 标注卖出数量
        for idx, pos in zip(sell_signals.index, sell_signals_position):
            quantity = sell_signals.loc[idx, 'amount']  # 假设数量列是 'quantity'
            ax_main.text(pos, sell_signals.loc[idx, 'close'] - 0.1,  # 在价格下方添加文本，-0.1调整位置
                         str(quantity),  # 作为文本标注数量
                         color='red', fontsize=8, ha='center', va='top')


def generate_macd_panel(df, panel_id=1):
    """
    生成MACD的副图
    这个只生成参数，使用的话还需要传入参数
    
    df, macd_panel = generate_macd_panel(df)
    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=macd_panel,
            panel_ratios=(3, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象
    """
    if 'DIF' not in df.columns:
        df['DIF'], df['DEA'], df['MACD'] = macd(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    
    macd_panel = [
                mpf.make_addplot(df['DIF'], panel=panel_id, color='b', alpha=0.5),
                mpf.make_addplot(df['DEA'], panel=panel_id, color='r', alpha=0.5),
                mpf.make_addplot(df['MACD'], panel=panel_id, color='grey', type='bar', width=0.7, alpha=0.5)
            ]
    
    return df, macd_panel

def generate_vwap_panel(df):
    if 'vwap' not in df.columns:
        df['vwap'] = vwap(df['close'], df['volume'])
    
    vwap_panel = [
        mpf.make_addplot(df['vwap'], color='orange', linestyle='solid', width=1.2, label='Average (VWAP)')
    ]
    
    return df, vwap_panel

def modify_macd_range(df, axes):
    macd_range = max(abs(df['DIF'].min()), abs(df['DIF'].max())) * 1.2
    macd_ylim = (-1 * macd_range, macd_range)
    for panel_id in range(1, 4):
        axes[panel_id].set_ylim(macd_ylim)

def plot_trade_history(df, trade_history):
    """
    :params
    df: quote of stock
    trade_history: trade signals
    """
    df = prepare_trade_history(df, trade_history)
    df, macd_panel = generate_macd_panel(df)
    df, vwap_panel = generate_vwap_panel(df)
    panels = macd_panel + vwap_panel
    
    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=panels,
            panel_ratios=(3, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象

    mark_bs_point(df, axes) # 标记买卖点
    modify_macd_range(df, axes) # 修正macd的range, 使极值绝对值相等

    # 显示图像
    plt.show()
    
def plot_debug_structure(df, trade_history):
    """
    debug structure
    :params
    df: quote of stock
    trade_history: trade signals
    """
    df = prepare_trade_history(df, trade_history)
    df, macd_panel = generate_macd_panel(df)
    df, vwap_panel = generate_vwap_panel(df)
    panels = macd_panel + vwap_panel + [
                mpf.make_addplot(df['DIF_scaled'], panel=2, color='b', alpha=0.5),
                mpf.make_addplot(df['DEA_scaled'], panel=2, color='r', alpha=0.5),
                mpf.make_addplot(df['MACD'], panel=2, color='grey', type='bar', width=0.7, alpha=0.5)
            ]
    
    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=panels,
            panel_ratios=(3, 1, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象

    mark_bs_point(df, axes)
    modify_macd_range(df, axes) # 修正macd的range, 使极值绝对值相等

    # 显示图像
    plt.show()
    
def plot_debug_rbreak(df, trade_history, rbreak):
    """
    debug structure
    :params
    df: quote of stock
    trade_history: trade signals
    """
    df = prepare_trade_history(df, trade_history)
    df, macd_panel = generate_macd_panel(df)
    df, vwap_panel = generate_vwap_panel(df)
    
    df['bBreak'] = rbreak.bBreak
    df['sSetup'] = rbreak.sSetup
    df['sEnter'] = rbreak.sEnter
    df['bEnter'] = rbreak.bEnter
    df['bSetup'] = rbreak.bSetup
    df['sBreak'] = rbreak.sBreak
    rbreak_lines = [
        mpf.make_addplot(df['bBreak'], panel=0, color='purple', linestyle='dashed', alpha=0.7, label='bBreak'),
        mpf.make_addplot(df['sSetup'], panel=0, color='red', linestyle='dashed', alpha=0.7, label='sSetup'),
        mpf.make_addplot(df['sEnter'], panel=0, color='blue', linestyle='dashed', alpha=0.7, label='sEnter'),
        mpf.make_addplot(df['bEnter'], panel=0, color='green', linestyle='dashed', alpha=0.7, label='bEnter'),
        mpf.make_addplot(df['bSetup'], panel=0, color='orange', linestyle='dashed', alpha=0.7, label='bSetup'),
        mpf.make_addplot(df['sBreak'], panel=0, color='black', linestyle='dashed', alpha=0.7, label='sBreak'),
    ]
    
    panels = macd_panel + vwap_panel + rbreak_lines
    
    fig, axes = mpf.plot(df, type='line', ylabel='Price', 
            addplot=panels,
            panel_ratios=(3, 1),  # 设置主图和副图的比例
            volume=False,         # 不显示成交量
            figsize=(10, 6),
            returnfig=True)       # 返回figure和axes对象

    mark_bs_point(df, axes)
    modify_macd_range(df, axes) # 修正macd的range, 使极值绝对值相等

    # 显示图像
    plt.show()