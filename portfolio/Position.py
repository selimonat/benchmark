import pandas as pd
from typing import SupportsFloat, AnyStr, Optional
from portfolio import Database
from portfolio import utils
db = Database.DB()


class Position:
    """
    Represents a position for a given security. Stores information (1) whether a position is open or closed,
    (2) the date of the transaction, (3) how many shares have been transacted, (4) the commission paid for this
    transaction (experimental). The price information is automatically filled unless it is manually given.

    Validates dates, ticker, action. Dates must be weekdays and ticker must be a valid ticker. A valid ticker is a
    ticker
    that is used by a given exchange market.
    """

    def __init__(self,
                 action: AnyStr,
                 quantity: float,  # fractional shares
                 ticker: AnyStr,
                 date: int,
                 cost: Optional[float] = None,
                 commision: Optional[float] = None):  # commission is currently in mock state.

        self.logger = utils.get_logger(__name__)
        self.action = action
        self.quantity = quantity
        if utils.is_valid_ticker(ticker):
            self.ticker = ticker
        else:
            raise Exception(f'{ticker} is not a valid ticker, I will not be able to retrieve asset value.')
        # You cannot open a position on a weekend.
        if not utils.is_weekend(date):
            self.date = date
        else:
            raise Exception(f'{date} is a weekend, I will not be able to retrieve asset value.')
        self.commission = 0 if commision is None else commision
        self.cost = self.value_at([self.date]) if cost is None else cost

    def __str__(self):
        return f"{self.action[:3]} {self.quantity:5} {self.ticker} for {self.cost:5.2f}$ ({self.commission:5.2f}$)" \
               f" {self.date}s " \
               f"(" \
               f"{utils.parse_epoch(self.date)})"

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
        out.cost = out.cost.astype(float)
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