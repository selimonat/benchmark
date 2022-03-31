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
    # TODO: exceptions where ticker doesn't exist must be handled.
    def read(self, ticker: str, date: list) -> list:
        """
        Reads value of a ticker at a given list of dates.
        Args:
            ticker: (str)
            date: (list) timestamps, epoch seconds

        Returns:
            list: values.
        """
        date = utils.ensure_iterable(date)
        self.logger.info(f'Reading {ticker} value at {date} from {self.connection}.')
        return [random.random() for _ in date]
