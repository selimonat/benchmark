from portfolio.Database import DB
from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Plotter import console_plot
from typing import List
import pandas as pd

db = DB()


class Portfolio:
    """
    Portfolio makes the book keeping of all transactions and time-course of asset prices.
    The class instance generates first a transaction table (indexed on transaction id) from the parsed export file.
    """

    def __init__(self, tickers: List[Ticker]):
        """

        Args:
            tickers: (Ticker) da`taframe resulting from parsing the export file.
        """
        self.tickers = tickers

    @property
    def returns(self):
        a = pd.concat([t.returns for t in self.tickers], axis=1)
        print(a)
        w = pd.concat([t.total_invested for t in self.tickers], axis=1)
        print(w)
        return (a * w).sum(axis=1, skipna=False) / w.sum(axis=1, skipna=False)