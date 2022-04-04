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
        self.table_transaction = pd.concat(out, axis=0).reset_index(drop=True)
        # table_transaction is indexed on transaction number.

    @property
    def table_asset_time_course(self):
        """
        This table represents time-course of asset prices. It is organized as [time, shares].
        Every single asset that is bought is represented as a separate column.
        Returns:
            df:
        """
        t = self.table_transaction.shape[0]  # number of transactions
        container = list()
        for i in range(t):  # run across transactions
            ticker = self.table_transaction.iloc[i].ticker  # get the ticker
            # create time index from position opening until today
            time_i = np.arange(self.table_transaction.iloc[i].date, self.current_time, (60 * 60 * 24))
            # get ticker price for these time points
            container.append(pd.DataFrame(db.read(ticker, list(time_i)),
                                          index=pd.Index(time_i, name='time'),
                                          columns=pd.Index([ticker])))
        return pd.concat(container, axis=1)

    # @property
    # def table_position_time_course(self):
    #
    #     # divide asset price in each column with its purchase price.
    #     self.table_asset_time_course * NORMALIZATION

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
