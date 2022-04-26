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
        out = {'portfolio value ($)': self.total_value_global,
               'portfolio returns (%)': self.current_portfolio_returns,
               'transactions': self.transactions_per_ticker,
               'current value': self.current_value_per_ticker,
               'percent change': self.percent_change_per_ticker,
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
    def portfolio_returns(self):
        a = pd.concat([t.tc_returns for t in self.tickers], axis=1)  # returns
        w = pd.concat([t.tc_invested for t in self.tickers], axis=1)  # weights
        s = (a * w).sum(axis=1, skipna=False) / w.sum(axis=1, skipna=False)  # weighted average.
        s.name = 'returns'
        return s

    @property
    def current_portfolio_returns(self):
        return self.portfolio_returns.iloc[-1].astype(float)

    @property
    def percent_change_per_ticker(self):
        # the most recent return value as computed by self.returns..
        return {t.ticker: t.tc_returns.loc[~t.tc_returns.isna()].iloc[-1] for t in self.tickers}

    @property
    def averaged_cost_per_ticker(self):
        return {t.ticker: t.current_average_cost_per_share for t in self.tickers}

    @property
    def total_shares_per_ticker(self):
        return {t.ticker: t.current_total_shares for t in self.tickers}

    @property
    def open_shares_per_ticker(self):
        return {t.ticker: t.current_open_shares for t in self.tickers}

    @property
    def closed_shares_per_ticker(self):
        return {t.ticker: t.current_closed_shares for t in self.tickers}

    @property
    def total_value_global(self):
        return np.sum([t.current_value for t in self.tickers])

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
