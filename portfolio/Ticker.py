from typing import Sequence
from portfolio.Position import Position
from portfolio.Database import DB
from portfolio import utils
import pandas as pd
import numpy as np

db = DB()


class ShareCounter:
    def __init__(self):
        self.shares = list()

    def add_new(self, n=1):
        # adds n new shares.
        [self.shares.append(len(self.shares) + 1) for _ in range(n)]

    def remove_old(self, n=1):
        # removes n old shares.
        [self.shares.pop(0) for _ in range(n)]

    def __call__(self):
        return self.shares


class Ticker:
    """
    An ensemble of positions under the same ticker label.
    Keeps track of 3 variables: investment, current_price and profit/loss, all aligned on the same time-axis.
    """

    def __init__(self, positions: Sequence[Position]):
        self.logger = utils.get_logger(__name__)
        self.positions = positions
        # check if all positions are from the same ticker
        ticker = np.unique([pos.ticker for pos in self.positions])
        if len(ticker) == 1:
            self.ticker = ticker[0]
        else:
            raise Exception("There are different tickers in the positions list...")
        self.ticker_value = db.read(self.ticker, self.time_line, output_format='series')

        self.shares = list()
        self.value = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.investment = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.profit_loss = pd.DataFrame(index=pd.Index(self.time_line, name='date'))

        self.update()

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
        # Implements FIFO logic.
        # Nanize values of time-points coming after the sell
        # it needs to find first non-nan time-point across the shares.
        for share in range(abs(pos.quantity)):
            current_share = self.investment.loc[pos.date].notna().idxmax()
            invalid = self.investment.index >= pos.date
            self.logger.info(f'All time points bigger than {pos.date} will be nanized for share {current_share}.')
            self.investment.loc[invalid, current_share] = np.nan
            self.value.loc[invalid, current_share] = np.nan
            self.profit_loss.loc[invalid, current_share] = np.nan

    @property
    def time_line(self):
        return np.arange(min([pos.date for pos in self.positions]),
                         utils.today(), (60 * 60 * 24),
                         dtype=int)

    @property
    def open_shares(self):
        return self.investment.iloc[-1, :].notna().sum()