"""Utility for read/write to running ES cluster."""
from typing import Optional, List, AnyStr, Union
import portfolio.utils as utils
from elasticsearch import Elasticsearch, helpers, NotFoundError
from elasticsearch_dsl import Index
import random
import pandas as pd
import uuid
import json
from downloader import utils

random.seed("die kartoffeln")


# TODO: exceptions where ticker doesn't exist must be handled. This requires a call to ES asking for existence of a
#  ticker. If yes, things should normally proceed and otherwise either fallback must be used or the exception in
#  db.read() must be handled.

# TODO: make a YF as an optional connector.

class DB:
    """
    Read-write interface to ES cluster for tickers.
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
             output_format: Optional[AnyStr] = "raw",
             fill_na: bool = True) -> Union[pd.Series, pd.DataFrame]:
        """
        Returns data for a given ticker either from the ES, fallbacks to direct call to YF.
        If a ticker is not stored in the DB, it will make a direct YF call and write the data to DB.

        Args:
            output_format: "raw" returns a dataframe, "series" returns a series, defaults to raw.
            index_name: name of the index_name, defaults to time-series.
            ticker: (str) A ticker symbol.
            date: (list) timestamps, epoch seconds. If not given complete historical data is returned.
            fill_na: (boolean) If True will fill-nan using pandas. Defaults to True.

        Returns:
            A series or df indexed on date with columns containing name of the ticker and its value.
        """

        # the default DF to return is an empty df.
        df = pd.DataFrame(data=1, columns=['ticker', 'Close'], index=pd.Index(date if date is not None else [],
                                                                              name='date'))
        df.ticker = df.ticker.astype("category")
        # this is what happens below:
        # - if the ES client is online, make a query.
        #   - if the ticker is in, return it.
        #   - if the ticker not present in db, then make a direct YF call and
        #       - write the results to ES.
        # - convert the df to series if requested and fillnan.

        if self.client.ping():  # if ES cluster reachable, get data from it:
            self.logger.info(f'Reading ticker {ticker} from index {index_name}')
            df = self.query_es(index_name, ticker, date)  # it could be that the ticker is not present.
            if df.empty:  # then make a direct YF call.
                self.logger.info(f"DB does not have this data stored, will make a direct YF call.")
                # call YF directly via downloader.
                df = utils.yf_call(ticker)
                self.logger.info(f"YF call  returned a df of size {df.shape}, we will cache this for future uses.")
                # cache it
                self.write(index_name, df)
                # here we could recall the query_es to return a standard DF, but ES is not fast enough to return
                # freshly written data, there are some asynchronous processes that leads to an empty return following
                # a read which comes right after a write.
                # df = self.query_es(index_name, ticker, date)
                df = pd.DataFrame(index=pd.Index(date, name='date')).join(df, how='left')
                df['ticker'] = ticker
                df.ticker = df.ticker.astype("category")  # need to do this again

        # if series is wanted than process it and convert it
        if output_format is "series":
            df = self.convert(df)

        if fill_na & (not df.empty):
            df.fillna(method='ffill', inplace=True)
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

    def query_es(self, index_name, ticker, date):
        # empty df with standard columns
        df = pd.DataFrame(columns=['ticker', 'Close'], index=pd.Index([], name='date'))
        # return all data
        res = helpers.scan(self.client,
                           index=index_name,
                           query={"query": {"match": {"ticker": ticker}}},
                           )
        # parse the raw results to pandas DF
        try:
            df = pd.DataFrame([r['_source'] for r in res])
            # Adapt ES output to benchmark standards.
            # dtype conversions:
            df.date = df.date.astype(int)
            df.Close = df.Close.astype(float)
            df.ticker = df.ticker.astype("category")

            df.set_index('date', inplace=True)
            df.index.name = 'date'
            df.columns.name = ticker
            # filter the requested time points
            if date is not None:
                # left join with a df that contains all the requested time points.
                # this will automatically create rows of NaN when the data was not present in the DB.
                # as a side-effect ticker column will also contain nans, that's why I overwrite it that column
                # TODO: one can directly ask for ES the requested dates rather than filtering them after the call.
                df = pd.DataFrame(index=pd.Index(date, name='date')).join(df, how='left')
                df['ticker'] = ticker
                df.ticker = df.ticker.astype("category")  # need to do this again

        except NotFoundError as err:
            self.logger.error(err)
        except AttributeError as err:
            self.logger.error(err)
        return df
