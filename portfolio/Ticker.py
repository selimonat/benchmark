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
    Keeps track of 3 variables: investment, value and profit/loss, all aligned on the same time-axis.
    mat_* are matrices organized as (time, n_shares)
    tc_* are column vectors representing time-courses.
    current_* are scalars or row vectors representing the most recent value.
    """

    def __init__(self, positions: Sequence[Position], value=None, clean_weekends=True, today=None):
        """
        Args:
            positions: List of Position classes.
            value: Use this for testing purposes to overwrite the mat_value ticker mat_value.
            clean_weekends: Boolean to filter out weekend days from the time line.
            today: Should the time_line be built using all the time points until today.
        """
        self.today = utils.today() if today is None else today
        self.clean_weekends = clean_weekends
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
        self.tc_ticker_value = db.read(self.ticker, self.time_line, output_format='series')
        #  TODO: if the returned mat_value is NaN because the db is not yet updated, then you will have columns for
        #   shares (bought "today") filled with nans. We need a procedure for avoiding this. One simply needs to
        #   remove full nan rows.

        if value is not None:
            value = utils.ensure_iterable(value)
            self.tc_ticker_value = pd.Series(data=value * len(self.time_line) if len(value) == 1 else value,
                                             index=self.time_line,
                                             )
        # TODO: Remove rows which are all nan.

        self.shares = list()
        self.mat_value = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.mat_investment = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        self.mat_profit_loss = pd.DataFrame(index=pd.Index(self.time_line, name='date'))
        # counter_* are incremented when shares are bought or sold. Their difference is the number of open shares.
        self.tc_counter_buy = pd.Series(data=0, index=pd.Index(self.time_line, name='date'), name='buy_counter')
        self.tc_counter_sell = pd.Series(data=0, index=pd.Index(self.time_line, name='date'), name='sell_counter')

        self.update()

        #  TODO: Raise an exception when a column is just NaN.

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
        """
        Updates mat_* matrices by adding new columns from the right side for each share.
        """
        for share in range(pos.quantity):  # TODO: implementation improvement, remove the for loop
            self.shares.append(len(self.shares))
            current_share = self.shares[-1]
            self.logger.info(f'Adding share {current_share}.')
            # fill up mat_investment
            self.mat_investment[current_share] = pos.cost
            invalid = self.mat_investment.index < pos.date  # invalid indices are prior to position opening.
            self.mat_investment.loc[invalid, current_share] = np.nan

            # fill up price
            self.mat_value[current_share] = self.tc_ticker_value
            self.mat_value.loc[invalid, current_share] = np.nan

            # fill up mat_profit_loss
            self.mat_profit_loss[current_share] = 0
            self.mat_profit_loss.loc[invalid, current_share] = np.nan

            # increment the share counter
            self.tc_counter_buy.loc[~invalid] += 1  # all valid points, including the date the position is open are
            # incremented by 1.

    def close_position(self, pos: Position):
        """
        Updates 3 dataframes: mat_investment, profit_lost, mat_value.
        Implements FIFO logic for selling shares ie sells shares that were bought first. For all sold shares,
        time-series are NaNized for all time-points coming after the sell transaction.

        """
        if len(self.shares) < abs(pos.quantity):
            raise ValueError('You do not have enough shares to sell.')

        for share in range(abs(pos.quantity)):
            current_share = self.mat_investment.loc[pos.date].notna().idxmax()
            invalid = self.mat_investment.index >= pos.date  # all time points after the positions are closed are
            # invalid.

            self.logger.info('Profit/Loss will be updated following this transaction')
            self.mat_profit_loss.loc[invalid, current_share] = \
                self.mat_value.loc[pos.date, current_share] - self.mat_investment.loc[pos.date, current_share]

            self.logger.info(f'All time points bigger than {pos.date} will be nanized for share {current_share}.')
            self.mat_investment.loc[invalid, current_share] = np.nan
            self.mat_value.loc[invalid, current_share] = np.nan

            # decrement the share counter
            self.tc_counter_sell.loc[invalid] += 1

    @property
    def time_line(self):
        """
        Computes the time index for all dataframes.
        It excludes weekends. It starts from the first date a position is open and stops at self.today.
        """
        step_size = (60 * 60 * 24)
        dummy = np.arange(min([pos.date for pos in self.positions]),
                          self.today + step_size,  # if step_size not added it will exclude
                          # today
                          step_size,
                          dtype=int) if len(self.positions) != 0 else []
        # exclude weekends
        weekends = [datetime.datetime.fromtimestamp(ts).weekday() <= 4 for ts in dummy]
        self.logger.info(f"Will remove {np.sum(weekends)} weekend days from the time line")
        return dummy[weekends] if self.clean_weekends else dummy

    # ######################################################
    # Parameters directly coming from the mat_* dataframes.
    # Time-Courses (TC).

    @property
    def tc_invested(self):
        # total money that has been invested.
        return self.mat_investment.sum(axis=1)

    @property
    def tc_cost(self):
        # synonym of invested
        return self.tc_invested

    @property
    def tc_profit_loss(self):
        return self.mat_profit_loss.sum(axis=1, skipna=True, min_count=1)

    @property
    def tc_value(self):
        # portfolio value
        return self.mat_value.sum(axis=1, skipna=True, min_count=1)

    # ######################################################
    # Derivative Parameters

    @property
    def tc_unrealized_gain(self):
        return (self.mat_value - self.mat_investment).sum(axis=1, skipna=True, min_count=1)

    @property
    def tc_returns(self):
        return 100 * self.tc_unrealized_gain / self.tc_cost

    @property
    def tc_average_cost_per_share(self):
        return self.tc_invested / self.tc_open_shares

    # ######################################################
    # Share counts:

    @property
    def tc_total_shares(self):
        # total number of shares that were ever transacted (buys + sells together)
        return self.tc_counter_buy

    @property
    def tc_open_shares(self):
        # number of shares that are currently open
        return self.tc_counter_buy-self.tc_counter_sell

    @property
    def tc_closed_shares(self):
        # number of shares that were sold
        return self.tc_counter_sell

    # ######################################################
    # Same as above but extract the CURRENT_VALUE ie take only the last value, this could be automatized at class level.
    # Get the last element of all tc_* properties.

    @property
    def current_value(self):
        # returns the last available mat_value .
        # using max as a convenience, there could be nans in the row
        return self.tc_value.iloc[-1].astype(float)

    @property
    def current_invested(self):
        return self.tc_invested.iloc[-1].astype(float)

    @property
    def current_cost(self):
        return self.tc_cost.iloc[-1].astype(float)

    @property
    def current_profit_loss(self):
        return self.tc_profit_loss.iloc[-1].sum().astype(float)

    @property
    def current_value(self):
        return self.tc_value.iloc[-1].astype(float)

    @property
    def current_ticker_value(self):
        return self.tc_ticker_value.iloc[-1].astype(float)

    @property
    def current_average_cost_per_share(self):
        return self.tc_average_cost_per_share.iloc[-1].astype(float)

    @property
    def current_open_shares(self):
        return self.tc_open_shares.iloc[-1].astype(float)

    @property
    def current_total_shares(self):
        return self.tc_counter_buy.iloc[-1].astype(float)

    @property
    def current_returns(self):
        return self.tc_returns.iloc[-1].astype(float)

    @property
    def current_unrealized_gain(self):
        return self.tc_unrealized_gain.iloc[-1].astype(float)

    @property
    def current_closed_shares(self):
        # number of shares that were sold up until today.
        return self.tc_counter_sell.iloc[-1].astype(float)
