import pandas as pd

from TradeApp import TradeApp
from StructureReserve import StructureReserve
from RBreak import RBreak

class StructureTradeApp(TradeApp):
    def on_bar_update(self, contract, bars, has_new_bar):
        if has_new_bar:
            bars = pd.DataFrame(bars)
            structure = StructureReserve()
            structure.update(contract, bars, self.pm)
            
class RBreakTradeApp(TradeApp):
    def on_bar_update(self, contract, bars, has_new_bar):
        if has_new_bar:
            bars = pd.DataFrame(bars)
            rbreak = RBreak(self.ib, contract, self.pm)
            rbreak.update(bars)
            
if __name__ == "__main__":            
    structure_ta = StructureTradeApp()
    structure_ta.subscribe_to_bars()
    
    # rbreak_ta = RBreakTradeApp()
    # rbreak_ta.subscribe_to_bars()
    