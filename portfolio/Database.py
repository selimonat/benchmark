"""Utility for read/write to running ES cluster. Currently mock."""
import portfolio.utils as utils
import random
random.seed("die kartoffeln")


class DB:
    """
    Currently mock situation. Currently connection is not implemented only random values for returned for all calls.
    Request values for tickers at specific dates.
    """
    def __init__(self, hostname):
        self.connection = hostname
        self.logger = utils.get_logger(self.__class__.__name__)

    # TODO: can declare optional types for date argument?
    def read(self, ticker: str, date) -> dict:
        """
        Value of ticker at date(s). Date can be an iterable.
        :param ticker: str
        :param date: integer or list of timestamps.
        :return: A dict as {date:value}
        """

        date = utils.ensure_iterable(date)
        self.logger.info(f'Reading {ticker} value at {date} from {self.connection}.')
        return {k: random.random() for k in date}
