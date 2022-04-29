from portfolio.Database import DB
from portfolio.Ticker import Ticker
from portfolio.Position import Position
from typing import List
import pandas as pd
import numpy as np
from collections import defaultdict
import copy

db = DB()


class Portfolio:
    """
    Portfolio makes the book keeping of all transactions and time-course of asset prices.
    The class instance generates first a transaction table (indexed on transaction id) from the parsed export file.
    """

    def __init__(self, tickers: List[Ticker], benchmark_symbol='GOOG'):
        """

        Args:
            tickers: (Ticker) dataframe resulting from parsing the export file.
        """
        self.benchmark_symbol = benchmark_symbol
        self.tickers = tickers
        # copy positions and replace all tickers with the benchmark ticker.
        dummy = copy.deepcopy(tickers)
        self.benchmark_positions = list()
        for t in dummy:
            for p in t.positions:
                # cost will be autofilled.
                self.benchmark_positions.append(
                    Position(ticker=self.benchmark_symbol,
                             action=p.action,
                             quantity=p.quantity,
                             date=p.date,
                             commision=p.commission)
                )
        # create now a ne ticker object from the modified positions
        self.benchmark_ticker = Ticker(self.benchmark_positions)

    @property
    def benchmark_returns(self):
        s = self.benchmark_ticker.tc_returns
        s.name = 'benchmark returns'
        return s

    @property
    def current_benchmark_returns(self):
        return self.benchmark_returns.iloc[-1].astype(float)

    @property
    def summary(self):
        out = {'portfolio value ($)': self.total_value_global,
               'portfolio returns (%)': self.current_portfolio_returns,
               'benchmark portfolio returns (%)': self.current_benchmark_returns,
               'transactions': self.transactions_per_ticker,
               'current value': self.current_value_per_ticker,
               'percent change': self.percent_change_per_ticker,
               'current number of shares': self.open_shares_per_ticker,
               'current sold shares': self.closed_shares_per_ticker,
               'total bought shares': self.total_shares_per_ticker,
               'average cost': self.averaged_cost_per_ticker,
               'profit/lost': self.profit_loss_per_ticker,
               'unrealized gain': self.unrealized_gain_per_ticker,
               'unfound tickers': []
               }
        return out

    # properties are organized to be specific either for tickers or for the portfolio (ie. all tickers).
    @property
    def portfolio_returns(self):
        a = pd.concat([t.tc_returns for t in self.tickers], axis=1)  # returns
        w = pd.concat([t.tc_invested for t in self.tickers], axis=1)  # weights
        s = pd.Series(np.average(a, axis=1, weights=w), index=a.index)  # weighted average.
        s.name = 'portfolio returns'
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
        # Returns list of positions for each ticker organized as a dict.
        out = defaultdict(list)
        for t in self.tickers:
            for p in t.positions:
                out['ticker'].append(p.__str__())
        return out

    @property
    def current_value_per_ticker(self):
        return {t.ticker: t.current_value for t in self.tickers}
