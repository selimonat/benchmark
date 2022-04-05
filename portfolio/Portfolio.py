from portfolio import utils
from portfolio import Database
import pandas as pd
from dateutil import parser
import numpy as np

db = Database.DB(hostname=None)


class Position:
    """
    Intermediate data structure between parsed export file and transaction table. If purchase price not present,
    it adds one. Represents the features associated with a single position.
    """
    def __init__(self, action: str, quantity: int, ticker: str, date: int):
        self.action = action
        self.quantity = quantity
        self.ticker = ticker
        self.date = date
        self.price = self.value_at([self.date])

    @property
    def df(self):
        """
        Returns:
            DataFrame: 1 row pandas view on the position with attributes as columns.
        """
        return pd.DataFrame.from_dict(self.__dict__)

    @property
    def value_yesterday(self) -> list:
        """
        Returns:
            float: Value of the position yesterday.
        """
        return self.value_at(utils.today())

    def value_at(self, date: list) -> list:
        """
        Value at an arbitrary date.
        Args:
            date: epoch seconds

        Returns:
            list: The value of the position
        """
        return db.read(self.ticker, date=date)


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
            time_i = np.arange(col.date,  utils.today(), (60 * 60 * 24))
            return pd.Series(db.read(ticker, list(time_i)),
                             index=pd.Index(time_i, name='time'),
                             name=ticker)

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
            time_i = np.arange(col.date,  utils.today(), (60 * 60 * 24))
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
            time_i = np.arange(col.date,  utils.today(), (60 * 60 * 24))
            return pd.Series(col.price,
                             index=pd.Index(time_i, name='time'),
                             name=ticker)

        out = self.table_transaction.copy().T
        out.columns = out.loc['ticker'].values
        return out.apply(expander_asset_cost).groupby(level=0, axis=1).mean()

    @property
    def table_time_course_asset_returns(self):
        return self.table_time_course_asset_price/self.table_time_course_asset_cost * 100

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
        return np.arange(self.start_time, self.current_time, (60*60*24), dtype=int)

    def returns(self):
        pass

    @staticmethod
    def plot(s: pd.Series):
        from uniplot import plot
        valid_index = (s.T.isna() == False)
        x = s.index.values[valid_index]
        y = s.T.values[valid_index]
        plot(xs=x,
             ys=y,
             lines=True,
             width=200)
