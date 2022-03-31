from portfolio import utils
from portfolio import Database
import pandas as pd

db = Database.DB(hostname=None)


class Position:
    def __init__(self, ticker: str, date: list):
        self.ticker = ticker
        self.open_date = date
        self.value_at_open = db.read(ticker, self.open_date)

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

    def returns_yesterday(self):
        """
        Returns of the position as a percentage.

        Returns:
            float: Returns
        """
        return self.value_yesterday[0] / self.value_at_open[0] * 100


class Portfolio:
    """
    Portfolio holds a bunch of positions and have methods to add and remove them. Each action triggers a recompute.
    """
    def __init__(self, tickers: list, dates: list):
        self.positions = pd.DataFrame()
        for ticker, date in zip(tickers,dates):
            self.buy(ticker, date)

    def buy(self, ticker, date):
        pass

    def sell(self, ticker, date):
        pass

    def returns(self):
        pass

