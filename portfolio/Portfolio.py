import random
import utils


class Position:
    def __init__(self, ticker, date):
        self.ticker = ticker
        self.open_date = date
        self.value_at_open = DB.read(ticker, self.open_date)

    @property
    def value_now(self):
        """
        Value of the position today.
        :return:
        """
        return DB.read(self.ticker, date=utils.today())

    def value_at(self, date):
        """
        Value at an arbitrary date.
        :param date: epoch seconds
        :return: float
        """
        return DB.read(self.ticker, date=date)

    def returns_at(self, date):
        """
        Returns returns as percentage for all dates.
        :param date:
        :return: percentage
        """
        return self.value_at_open / self.value_now * 100



class Portfolio:
    def __init__(self):
        self.position = []
    def