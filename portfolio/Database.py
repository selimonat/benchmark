"""Utility for read/write to running ES cluster."""
from typing import Optional, List
import portfolio.utils as utils
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Index
import random
import pandas as pd

random.seed("die kartoffeln")


class DB:
    """
    Currently mock situation. Currently client is not implemented only random values for returned for all calls.
    Request values for tickers at specific dates.
    """

    def __init__(self, hostname="localhost:9200"):
        self.hostname = hostname
        self.client = Elasticsearch("http://elastic:changeme@" + self.hostname)
        self.logger = utils.get_logger(self.__class__.__name__)

    def setup_es_index(self, index_name: str):
        """Creates indices with INDEX_NAME using elasticsearch CLIENT"""
        if not self.client.indices.exists(index=index_name):
            self.logger.info(f"Setting up index {index_name} on {self.client.info()['cluster_name']}.")
            index = Index(index_name, self.client)
            index.settings(
                number_of_shards=1,
                number_of_replicas=1, )
            # ignore already exists error
            index.create(ignore=400)
            return True
        else:
            return False

    def read(self, ticker: str, date: Optional[List] = None) -> pd.Series:
        """
        Returns data for a given ticker. If no date is given all data is returned.
        Args:
            ticker: (str) A Nasdaq ticker
            date: (list) timestamps, epoch seconds.

        Returns:
            series: A pandas Series named "price" showing time-course of a ticker, indexed on time.
        TODO: one can directly ask for ES the requested dates rather than filtering them after the call.
        """
        q = {"query": {"match": {"ticker": ticker}}}
        i = "time-series"
        # return all data
        res = helpers.scan(self.client,
                           index=i,
                           query=q,
                           )
        # parse it to pandas
        df = pd.DataFrame([r['_source'] for r in res])
        # preprocess
        df.set_index('date', inplace=True)
        df.rename(columns={'Close': 'price'}, inplace=True)
        df = df['price']
        # # filter by time
        if date is not None:
            return df.loc[df.index.isin(date)]
        return df

    # def write(self, df):
    #     pass

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
    db.read(ticker='AAPL', date=[1648598400])
