from portfolio import utils
from portfolio import Database
import pandas as pd

db = Database.DB(hostname=None)


class Position:
    """
    Intermediate data structure between parsed export file and transaction table. If purchase price not present,
    it adds one.
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
    Portfolio makes the book keeping of all transactions.
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
        self.transactions = pd.concat(out, axis=0)

    def buy(self, ticker, date, amount):
        pass

    def sell(self, ticker, date, amount):
        pass

    def returns(self):
        pass


if __name__ == '__main__':
    p = Portfolio(actions=['buy', 'buy'],
                  quantities=[2, 3],
                  tickers=['babs', 'tops'],
                  dates=[134124, 13413])