from portfolio import utils
from portfolio import Database
import pandas as pd

db = Database.DB(hostname=None)


class Position:
    def __init__(self, action: str, amount: int, ticker: str, date: int):
        self.action = action
        self.amount = amount
        self.ticker = ticker
        self.open_date = date
        self.value_at_open = db.read(ticker, [self.open_date])

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
    Portfolio holds a bunch of positions and have methods to add and remove them. Each action triggers a recompute.
    """
    def __init__(self, actions: list, amounts: list, tickers: list, dates: list):

        out = [Position(act, am, t, d).df for act, am, t, d in zip(actions, amounts, tickers, dates)]
        self.transactions = pd.concat(out, axis=0)

    def buy(self, ticker, date, amount):
        pass

    def sell(self, ticker, date, amount):
        pass

    def returns(self):
        pass


if __name__ == '__main__':
    p = Portfolio(actions=['buy', 'buy'],
                  amounts=[2, 3],
                  tickers=['babs', 'tops'],
                  dates=[134124, 13413])