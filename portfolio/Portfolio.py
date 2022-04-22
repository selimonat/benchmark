from portfolio.Database import DB
from portfolio.Ticker import Ticker
from typing import List
import pandas as pd
import numpy as np
from collections import defaultdict

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
        out = {'transactions': self.transactions_per_ticker,
               'current value': self.current_value_per_ticker,
               'percent change': self.percent_change_per_ticker,
               'portfolio value': self.total_value_global,
               'current number of shares': self.open_shares_per_ticker,
               'current sold shares': self.closed_shares_per_ticker,
               'total bought shares': self.total_shares_per_ticker,
               'average cost': self.averaged_cost_per_ticker,
               'profit/lost': self.profit_loss_per_ticker,
               'unrealized gain': self.unrealized_gain_per_ticker,
               }
        return out

    # properties are organized to be specific either for tickers or for the portfolio (ie. all tickers).
    @property
    def returns_global(self):
        a = pd.concat([t.returns for t in self.tickers], axis=1)  # returns
        w = pd.concat([t.total_invested for t in self.tickers], axis=1)  # weights
        s = (a * w).sum(axis=1, skipna=False) / w.sum(axis=1, skipna=False)  # weighted average.
        s.name = 'returns'
        return s

    @property
    def percent_change_per_ticker(self):
        # the most recent return value as computed by self.returns..
        return {t.ticker: t.returns.loc[~t.returns.isna()].iloc[-1] for t in self.tickers}

    @property
    def averaged_cost_per_ticker(self):
        return {t.ticker: t.total_invested.iloc[-1] / t.total_shares for t in self.tickers}

    @property
    def total_shares_per_ticker(self):
        return {t.ticker: t.total_shares for t in self.tickers}

    @property
    def open_shares_per_ticker(self):
        return {t.ticker: t.current_open_shares for t in self.tickers}

    @property
    def closed_shares_per_ticker(self):
        return {t.ticker: t.current_sold_shares for t in self.tickers}

    @property
    def total_value_global(self):
        return np.sum([t.total_invested.loc[~t.returns.isna()].iloc[-1] for t in self.tickers])

    @property
    def profit_loss_per_ticker(self):
        return {t.ticker: t.current_profit_loss for t in self.tickers}

    @property
    def unrealized_gain_per_ticker(self):
        return {t.ticker: t.current_unrealized_gain for t in self.tickers}

    @property
    def transactions_per_ticker(self):
        out = defaultdict(list)
        for t in self.tickers:
            for p in t.positions:
                out['ticker'].append(p.__str__())
        return out

    @property
    def current_value_per_ticker(self):
        return {t.ticker: t.current_value for t in self.tickers}
