from typing import Sequence
from portfolio.Position import Position
from portfolio.Database import DB
from portfolio import utils
import pandas as pd
import numpy as np
import datetime

db = DB()


class Ticker:
    """
    An ensemble of positions under the same ticker label.
    Keeps track of 3 variables: investment, current_price and profit/loss, all aligned on the same time-axis.
    """

    def __init__(self, positions: Sequence[Position], value=None):
        """
        Args:
            positions: List of Position classes.
            value: Use this for testing purposes to overwrite the value ticker value.
        """
        self.logger = utils.get_logger(__name__)
        self.positions = positions
        # check if all positions are from the same ticker
        ticker = np.unique([pos.ticker for pos in self.positions])
        if len(ticker) == 1:
            self.ticker = ticker[0]
        elif len(ticker) > 1:
            raise Exception("There are different tickers in the positions list...")
        else:
            raise Exception("No positions are given...")
        self.ticker_value = db.read(self.ticker, self.time_line, output_format='series')
        if value is not None:
            self.ticker_value = pd.Series(data=value,
                                          index=self.time_line,
                                          )

        self.shares = list()
        self.value = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.investment = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.profit_loss = pd.DataFrame(index=pd.Index(self.time_line, name='date'))

        self.update()

    def __str__(self):
        return ''.join([f"Pos#{i}: {pos.__str__()}\n" for i, pos in enumerate(self.positions)])

    def update(self):

        for pos in self.positions:
            if pos.action == "buy":
                self.logger.info('Buying positions')
                self.add_position(pos)
            elif pos.action == "sell":
                self.logger.info('Selling positions.')
                self.close_position(pos)

    def add_position(self, pos: Position):
        # A position can have multiple shares, run across them.
        #  TODO: implementation improvement, remove the for loop
        for share in range(pos.quantity):
            self.shares.append(len(self.shares))
            current_share = self.shares[-1]
            self.logger.info(f'Adding share {current_share}.')
            # fill up investment
            self.investment[current_share] = pos.cost
            invalid = self.investment.index < pos.date
            self.investment.loc[invalid, current_share] = np.nan

            # fill up price
            self.value[current_share] = self.ticker_value
            self.value.loc[invalid, current_share] = np.nan

            # fill up profit_loss
            self.profit_loss[current_share] = 0
            self.profit_loss.loc[invalid, current_share] = np.nan

    def close_position(self, pos: Position):
        """
        # Implements FIFO logic for selling shares ie sells shares that were bought first. For all sold shares,
        time-series are NaNized for all time-points coming after the sell transaction.

        Args:
            pos: (Position)

        Returns:
            Updates 3 dataframes: investment, profit_lost, value.
        """
        if len(self.shares) < abs(pos.quantity):
            raise ValueError('You do not have enough shares to sell.')

        for share in range(abs(pos.quantity)):
            current_share = self.investment.loc[pos.date].notna().idxmax()
            invalid = self.investment.index >= pos.date

            self.logger.info('Profit/Loss will be updated following this transaction')
            self.profit_loss.loc[invalid, current_share] = \
                self.value.loc[invalid, current_share] - self.investment.loc[invalid, current_share]

            self.logger.info(f'All time points bigger than {pos.date} will be nanized for share {current_share}.')
            self.investment.loc[invalid, current_share] = np.nan
            self.value.loc[invalid, current_share] = np.nan

    @property
    def time_line(self):
        """
        Computes the time index for all dataframes. It excludes weekends.
        """
        step_size = (60 * 60 * 24)
        dummy = np.arange(min([pos.date for pos in self.positions]),
                          utils.today() + step_size,  # if step_size not added it will exclude today
                          step_size,
                          dtype=int) if len(self.positions) != 0 else []
        # exclude weekends
        weekends = [datetime.datetime.fromtimestamp(ts).weekday() <= 4 for ts in dummy]
        return dummy[weekends]

    @property
    def current_open_shares(self):
        return self.investment.iloc[-1, :].notna().sum().astype(float)

    @property
    def total_shares(self):
        # both closed and open shares.
        return self.investment.shape[1]

    @property
    def current_sold_shares(self):
        return self.investment.iloc[-1, :].isna().sum().astype(float)

    @property
    def total_invested(self):
        # total money that is currently invested.
        return self.investment.sum(axis=1)

    @property
    def returns(self):
        I = self.investment.sum(axis=1, skipna=True, min_count=1)
        V = self.value.sum(axis=1, skipna=True, min_count=1)
        return 100 * (V - I) / I

    @property
    def unrealized_gain(self):
        return (self.value - self.investment).sum(axis=1)

    @property
    def average_cost_per_share(self):
        return self.total_invested / self.total_shares
