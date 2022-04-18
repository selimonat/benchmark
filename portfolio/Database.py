"""Utility for read/write to running ES cluster."""
from typing import Optional, List, AnyStr, Union
import portfolio.utils as utils
from elasticsearch import Elasticsearch, helpers, NotFoundError
from elasticsearch_dsl import Index
import random
import pandas as pd
import uuid
import json

random.seed("die kartoffeln")


# TODO: exceptions where ticker doesn't exist must be handled. This requires a call to ES asking for existence of a
#  ticker. If yes, things should normally proceed and otherwise either fallback must be used or the exception in
#  db.read() must be handled.


class DB:
    """
    Currently mock situation. Currently client is not implemented only random values for returned for all calls.
    Request values for tickers at specific dates.
    """

    def __init__(self, hostname="localhost:9200"):
        self.hostname = hostname
        self.client = Elasticsearch("http://elastic:changeme@" + self.hostname)
        self.logger = utils.get_logger(self.__class__.__name__)

    def setup_es_index(self, index_name: str) -> bool:
        """Creates indices with INDEX_NAME using elasticsearch CLIENT"""
        if not self.client.indices.exists(index=index_name):
            self.logger.info(f"Setting up index_name {index_name} on {self.client.info()['cluster_name']}.")
            index = Index(index_name, self.client)
            index.settings(
                number_of_shards=1,
                number_of_replicas=1, )
            # ignore already exists error
            index.create(ignore=400)
            self.logger.info("done")
            return True
        else:
            self.logger.info("Index exists already")
            return False

    def read(self, ticker: str,
             date: Optional[List] = None,
             index_name: Optional[AnyStr] = "time-series",
             output_format: Optional[AnyStr] = "raw") -> Union[pd.Series, pd.DataFrame]:
        """
        Returns data for a given ticker. If no date is given all data is returned.
        Args:
            output_format: "raw" returns a dataframe, "series" returns a series, defaults to raw
            index_name: name of the index_name, defaults to time-series
            ticker: (str) A Nasdaq ticker
            date: (list) timestamps, epoch seconds.

        Returns:
            series: A pandas Series named "price" showing time-course of a ticker, indexed on time.
        TODO: one can directly ask for ES the requested dates rather than filtering them after the call.
        TODO: it is generally a better approach to have the output data set to be indexed or at least to contain the
              required date range. Currently when the requested date is not stored in the db, the returned data
              structure is empty, does not contain any trace of the requested date. It would be better to have the
              index stored but to contain nan values.
        """
        q = {"query": {"match": {"ticker": ticker}}}

        # return all data
        res = helpers.scan(self.client,
                           index=index_name,
                           query=q,
                           )
        # empty df with standard columns
        df = pd.DataFrame(columns=['ticker', 'Close'], index=pd.Index([], name='date'))
        # parse the raw results to pandas DF
        try:
            df = pd.DataFrame([r['_source'] for r in res])
            # dtype conversions:
            df.date = df.date.astype(int)
            df.Close = df.Close.astype(float)
            df.ticker = df.ticker.astype("category")

            df.set_index('date', inplace=True)
            df.index.name = 'date'
            df.columns.name = ticker
            # filter the requested time points
            if date is not None:
                df = df.loc[df.index.isin(date)]

            # if series is wanted than process it and convert it
            if output_format is "series":
                df = self.convert(df)

        except NotFoundError as err:
            self.logger.error(err)
        except AttributeError as err:
            self.logger.error(err)
        return df

    def write(self, index_name: AnyStr, df: pd.DataFrame) -> bool:
        """
        Sends data to ES.
        TODO: What happens if the same data is already there?
        Args:
            index_name: Index name
            df: Bulk-sends a pandas Series to ES.

        Returns:
            Response object from bulk call.
        """
        # check validity
        if not set(df.reset_index().columns) == {'date', 'Close', 'ticker'}:
            self.logger.error('Required columns are not present.')
            return False

        actions = [
            {
                "_index": index_name,
                "_type": "_doc",
                "_id": uuid.uuid4(),
                "_source": json.dumps(node)
            }
            for node in df.reset_index().to_dict(orient='records')
        ]
        try:
            response = helpers.bulk(self.client, actions)
            self.logger.info(f"Bulk sending data to ES, got this {response}.")
            return True
        except:
            self.logger.error(f"Bulk sending data to ES failed")
            return False

    def convert(self, data: Union[pd.Series, pd.DataFrame]):
        """
        Bi-directional converter.
        Args:
            data:

        Returns:

        """
        if type(data) is pd.DataFrame:  # convert to Series
            self.logger.info("DataFrame received, will convert it to Series")
            # preprocess
            ticker = data.columns.name
            s = data['Close']
            s.name = ticker
            s.index.name = 'date'
            return s

        elif type(data) is pd.Series:  # convert to dataframe
            self.logger.info("Series received, will convert it to DataFrame")
            ticker = data.name
            df_ = data.to_frame()
            df_.rename(columns={ticker: 'Close'}, inplace=True)
            df_['ticker'] = ticker
            df_.index = df_.index.astype(int)
            df_.Close = df_.Close.astype(float)
            df_.ticker = df_.ticker.astype("category")
            return df_

    def random_read(self, ticker: str, date: list) -> list:
        """
        TODO: Use this as a fallback maybe in the future, or delete it.
        Generates random price values for a given ticker.
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
