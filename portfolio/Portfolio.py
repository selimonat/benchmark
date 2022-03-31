from portfolio import utils
from portfolio import Database

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


# class Portfolio:
#     def __init__(self):
#         self.position = []
#
#     def
