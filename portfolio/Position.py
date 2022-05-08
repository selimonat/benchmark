import pandas as pd
from numpy import isnan
from typing import SupportsFloat, AnyStr, Optional
from portfolio import Database
from portfolio import utils
import numpy as np

db = Database.DB()


class Position:
    """
    Represents a position for a given security. Stores information (1) whether a position is open or closed,
    (2) the date of the transaction, (3) how many shares have been transacted, (4) the commission paid for this
    transaction (experimental). The price information is automatically filled unless it is manually given.

    Validates dates, ticker, action and quantity. Dates must be weekdays and ticker must be a valid ticker. A valid
    ticker is a ticker that is used by a given exchange market.
    """
    logger = utils.get_logger(__name__)

    def __init__(self,
                 action: AnyStr,
                 quantity: float,  # fractional shares
                 ticker: AnyStr,
                 date: int,
                 cost: Optional[float] = None,
                 commission: Optional[float] = None):  # commission is currently in mock state.

        self.logger.info(f"Position object is being created with action: {action}, quantity: {quantity}, "
                         f"ticker: {ticker}, date: {date}, cost: {cost}, commission: {commission} parameters")
        self.action = action.lower()
        if not isnan(quantity):
            self.quantity = int(np.ceil(quantity))  # TODO: need work for fractionalization.
        else:
            raise Exception(f"{ticker} at {date} cannot have nan quantity.")

        if utils.is_valid_ticker(ticker):
            self.ticker = ticker
        else:
            raise Exception(f'{ticker} is not a valid ticker, I will not be able to retrieve asset value.')
        # You cannot open a position on a weekend.
        if not utils.is_weekend(date):
            self.date = date
        else:
            raise Exception(f'{date} is a weekend, I will not be able to retrieve asset value.')
        self.commission = 0 if commission is None else commission
        self.cost = self.value_at([self.date]) if cost is None else cost

    def __str__(self):
        return f"{self.action[:3]} {self.quantity:10.2f} shares of {self.ticker:10} for {self.cost:10.2f}$ " \
               f"({self.commission:5.2f}$) {self.date}s ({utils.parse_epoch(self.date)})"

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
        self.logger.info(f"Attempt to get the value of {self.ticker} at {self.date}.")
        out = db.read(ticker=self.ticker, date=date, output_format='series')
        self.logger.debug(f"Received a df of shape {out.shape}:\n {out}")
        if (out.shape[0] == 1) and (not out.isna().any()):  # if there is data and it is not null.
            return out.iloc[0]
        elif (out.shape[0] == 0) or out.isna().any():  # if there is no data or it is a nan
            self.logger.info(f"No value found for {self.ticker} at {self.date}, will try surrounding dates.")

            dates = list()
            for n in [-3, -2, -1, 0, 1, 2, 3]:
                dates.append(date[0] - 24 * 60 * 60 * n)
            out = db.read(ticker=self.ticker, date=dates, output_format='series').loc[date]
            # if STILL there is no data or it is a nan
            if (out.shape[0] == 0) or out.isna().any():
                raise Exception(f"Cannot find the sell/buy value of the position for {self.ticker} at {self.date}.")
            else:
                return out.iloc[0]
        else:
            self.logger.info(f"This is the dataframe returned:\n{out}")
            raise Exception("The output should only be at most of length 1")
