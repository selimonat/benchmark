from portfolio.Database import DB
from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Plotter import console_plot
from typing import List
import pandas as pd
import json

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
        w = pd.concat([t.total_invested for t in self.tickers], axis=1)
        s = (a * w).sum(axis=1, skipna=False) / w.sum(axis=1, skipna=False)
        s.name = 'returns'
        return s

    @property
    def summary(self):
        out = {'return': {t.ticker: t.returns.loc[~t.returns.isna()].iloc[-1] for t in self.tickers},
               'total shares': {t.ticker: t.total_shares for t in self.tickers},
               'average cost': {t.ticker: t.total_invested.iloc[-1] / t.total_shares for t in self.tickers}
               }
        return json.dumps(out)
