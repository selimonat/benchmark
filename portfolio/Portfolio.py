from portfolio.Database import DB
from portfolio.Ticker import Ticker
from typing import List
import pandas as pd
import numpy as np

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
    def summary(self):
        out = {'percent change': self.percent_change,
               'portfolio value': self.total_value,
               'current number of shares': self.open_shares,
               'current sold shares': self.closed_shares,
               'average cost': {t.ticker: t.total_invested.iloc[-1] / t.total_shares for t in self.tickers}
               }
        return out

    @property
    def returns(self):
        a = pd.concat([t.returns for t in self.tickers], axis=1)  # returns
        w = pd.concat([t.total_invested for t in self.tickers], axis=1)  # weights
        s = (a * w).sum(axis=1, skipna=False) / w.sum(axis=1, skipna=False)  # weighted average.
        s.name = 'returns'
        return s

    @property
    def percent_change(self):
        # the most recent return value as computed by self.returns..
        return {t.ticker: t.returns.loc[~t.returns.isna()].iloc[-1] for t in self.tickers}

    @property
    def total_shares(self):
        return {t.ticker: t.total_shares for t in self.tickers}

    @property
    def open_shares(self):
        return {t.ticker: t.current_open_shares for t in self.tickers}

    @property
    def closed_shares(self):
        return {t.ticker: t.current_sold_shares for t in self.tickers}

    @property
    def total_value(self):
        return np.sum([t.total_invested.loc[~t.returns.isna()].iloc[-1] for t in self.tickers])
