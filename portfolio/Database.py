"""Utility for read/write to running ES cluster. Currently mock."""
import portfolio.utils as utils
from elasticsearch import Elasticsearch, helpers
import random
import pandas as pd

random.seed("die kartoffeln")


class DB:
    """
    Currently mock situation. Currently connection is not implemented only random values for returned for all calls.
    Request values for tickers at specific dates.
    """

    def __init__(self, hostname="localhost:9200"):
        self.hostname = hostname
        self.connection = Elasticsearch("http://elastic:changeme@" + self.hostname)
        self.logger = utils.get_logger(self.__class__.__name__)

    def read(self, ticker: str) -> pd.Series:
        q = {"query": {"match": {"ticker": ticker}}}
        i = "time-series"
        res = helpers.scan(self.connection,
                           index=i,
                           query=q,
                           )
        df = pd.DataFrame([r['_source'] for r in res])
        df.set_index('date', inplace=True)
        df.rename(columns={'Close': 'price'}, inplace=True)
        df = df['price']
        return df

    # TODO: can declare optional types for date argument?
    # TODO: exceptions where ticker doesn't exist must be handled.
    def random_read(self, ticker: str, date: list) -> list:
        """
        Reads value of a ticker at a given list of dates.
        Args:
            ticker: (str)
            date: (list) timestamps, epoch seconds

        Returns:
            list: values.
        """
        date = utils.ensure_iterable(date)
        self.logger.debug(f'Reading {ticker} value at {len(date)} different values (min: {min(date)}, max: {max(date)})'
                          f' from random number generator.')
        return [random.random() for _ in date]


if __name__ is '__main__':
    db = DB()
    db.read(ticker='AAPL')
