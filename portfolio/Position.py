import pandas as pd
from typing import SupportsFloat, AnyStr, SupportsInt
from portfolio import Database
from portfolio import utils
db = Database.DB()


class Position:
    """
    Represents an open or closed position for a given ticker, stores information about whether a position is open or
    closed, how many shares have been sold and the date of purchase or close.
    """

    def __init__(self, action: AnyStr, quantity: int, ticker: AnyStr, date: int):
        self.logger = utils.get_logger(__name__)
        self.action = action
        self.quantity = quantity
        self.ticker = ticker
        self.date = date
        self.commission = 0
        self.current_price = self.value_at([self.date])

    def __str__(self):
        return f"{self.action} of {self.quantity} shares of {self.ticker} for {self.current_price} at {self.date}.  " \
               f"It has been paid a commision of {self.commission} for this position."

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns:
            DataFrame: 1 row pandas view on the position with attributes as columns.
        """
        out = pd.DataFrame.from_dict([self.__dict__])
        # conversion of data types
        out.action = out.action.astype('category')
        out.quantity = out.quantity.astype(int)
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