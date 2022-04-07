from portfolio import utils
from portfolio import Database
import pandas as pd
from dateutil import parser
import numpy as np
from typing import SupportsFloat

db = Database.DB()


class Position:
    """
    Intermediate data structure between parsed export file and transaction table. If purchase price not present,
    it adds one. Represents the features associated with a single position.
    """

    def __init__(self, action: str, quantity: int, ticker: str, date: int):
        self.logger = utils.get_logger(__name__)
        self.action = action
        self.quantity = quantity
        self.ticker = ticker
        self.date = date
        self.price = self.value_at([self.date])

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns:
            DataFrame: 1 row pandas view on the position with attributes as columns.
        """
        out = pd.DataFrame.from_dict([self.__dict__])
        # conversion of data types
        out.action = out.action.astype('category')
        out.quantity = out.quantity.astype(float)
        out.ticker = out.ticker.astype('category')
        out.date = out.date.astype(int)
        out.price = out.price.astype(float)
        return out

    def value_at(self, date: list) -> SupportsFloat:
        """
        Value at an arbitrary date.
        Args:
            date: epoch seconds

        Returns:
            list: The value of the position. Returns NaN if no value.
        """
        out = db.read(ticker=self.ticker, date=date, output_format='series')
        if out.shape[0] == 1:
            return out.iloc[0]
        elif out.shape[0] == 0:
            self.logger.info(f"No value found for {self.ticker} at {self.date}, returning NaN")
            return float('nan')
        else:
            raise Exception("The output should only be at most of length 1")


class Portfolio:
    """
    Portfolio makes the book keeping of all transactions and time-course of asset prices.
    The class instance generates first a transaction table (indexed on transaction id) from the parsed export file.
    """

    def __init__(self, df):
        """

        Args:
            df: dataframe resulting from parsing the export file.
        """
        actions = df.action
        quantities = df.quantity
        tickers = df.ticker
        dates = df.date

        out = [Position(act, am, t, d).df for act, am, t, d in zip(actions, quantities, tickers, dates)]

        df = pd.concat(out, axis=0).reset_index(drop=True)
        # assign a lot id to each transaction
        self.table_transaction = utils.group_rank_id(df, 'ticker', ['ticker', 'date']).rename(columns={'rank': 'lot'})
        self.table_transaction['lot'] = self.table_transaction['lot'] + 1

    @property
    def table_time_course_asset_price(self):
        """
        This table expands the transaction table in time to represent time-course of asset prices. Every single asset
        that is bought is represented as a separate column.
        Returns:
            df: organized as [time, asset-names].
        """

        def expander_asset_price(col: pd.Series):
            """Expands each row in transaction table across time where each time point represents the market price of an
            asset.
            """
            ticker = col.ticker
            time_i = np.arange(col.date, utils.today(), (60 * 60 * 24))
            return db.read(ticker, date=time_i, output_format='series')

        #  need this intermediate variable to have correct column names after apply(), otherwise they follow the
        #  indices of the transaction table.
        out = self.table_transaction.copy().T
        out.columns = out.loc['ticker'].values
        # transaction table => Expand in time
        return out.apply(expander_asset_price).groupby(level=0, axis=1).mean()

    @property
    def table_time_course_asset_quantity(self):
        """
        This table expands the transaction table to keep track of the number of assets purchased at any given time
        point. It is aligned with the table_asset_time_course.
        Returns:

        """

        def expander_asset_quantity(col: pd.Series):
            """Expands transaction table across time where each time point represents the cost of assets"""
            ticker = col.ticker
            time_i = np.arange(col.date, utils.today(), (60 * 60 * 24))
            return pd.Series(1,
                             index=pd.Index(time_i, name='time'),
                             name=ticker)

        out = self.table_transaction.copy().T
        out.columns = out.loc['ticker'].values
        return out.apply(expander_asset_quantity).groupby(level=0, axis=1).sum()

    @property
    def table_time_course_asset_cost(self):
        def expander_asset_cost(col: pd.Series):
            """Expands transaction table across time where each time point represents the number of own assets"""
            ticker = col.ticker
            time_i = np.arange(col.date, utils.today(), (60 * 60 * 24))
            return pd.Series(col.price,
                             index=pd.Index(time_i, name='time'),
                             name=ticker)

        out = self.table_transaction.copy().T
        out.columns = out.loc['ticker'].values
        return out.apply(expander_asset_cost).groupby(level=0, axis=1).mean()

    @property
    def table_time_course_asset_returns(self):
        return self.table_time_course_asset_price / self.table_time_course_asset_cost * 100

    # @property
    # def table_time_course_portfolio_returns(self):
    #     pass
    # return self.table_time_course_asset_returns

    @property
    def start_time(self):
        # Start time of the portfolio, typically earliest opened position
        return parser.parse("20200101T000000 UTC").timestamp()

    @property
    def current_time(self):
        # Returns current day as timestamp in UTC
        return utils.today()

    @property
    def time_line(self):
        # The time axis of the portfolio. step size is day.
        return np.arange(self.start_time, self.current_time, (60 * 60 * 24), dtype=int)

    def returns(self):
        pass

    @staticmethod
    def plot(s: pd.Series):
        from uniplot import plot
        valid_index = ~s.T.isna()
        x = s.index.values[valid_index]
        y = s.T.values[valid_index]
        plot(xs=x,
             ys=y,
             lines=True,
             width=150)
