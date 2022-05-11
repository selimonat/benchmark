"""Utility for read/write to running ES cluster."""
from typing import Optional, List, AnyStr, Union
import portfolio.utils as utils
from elasticsearch import Elasticsearch, helpers, NotFoundError
from elasticsearch_dsl import Index, Search
import random
import pandas as pd
import json
from downloader import utils
from time import sleep
from portfolio import es_query_collection as qe

random.seed("die kartoffeln")


class DB:
    """
    Read-write interface to ES cluster for tickers.
    """
    logger = utils.get_logger(__name__)

    def __init__(self, hostname="localhost:9200"):
        self.hostname = hostname
        self.client = Elasticsearch("http://elastic:changeme@" + self.hostname)

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

        # the DF to return when ES cluster is not reachable. Making it non-empty makes it possible to run unittests.
        df = pd.DataFrame(data=1,
                          columns=pd.Index(['ticker', 'Close'], name=ticker),
                          index=pd.Index(date if date is not None else [], name='date'))
        df.ticker = df.ticker.astype("category")

        if self.client.ping():  # if ES cluster reachable, get data from it:
            self.setup_es_index(index_name=index_name)  # TODO: move this to the init stage
            # there are 3 possible outcomes here if the db is online:
            # (1) df is returned empty because it was not in the database.
            # (2) df is not empty, but does not contain data at the required date.
            # (3) df is not empty, and it contains the required date.
            self.logger.info(f'Reading ticker {ticker} from index {index_name}')
            df = self.query_es(index_name, ticker, date)
            #  Duplicate detection
            if df.index.duplicated().sum() > 0:
                raise Exception(f'There are duplicated in the DB {ticker}, asked for {len(date)} date points but got '
                                f'{df.shape[0]}.')

            if df.empty or df.loc[:, 'Close'].isna().all():
                self.logger.info(f"DB does not contain data for {ticker} at the required date {date}, "
                                 f"will make a direct YF call.")
                # call YF directly via downloader.
                df = utils.yf_call(ticker)
                self.logger.info(f"YF call  returned a df of size {df.shape}, we will cache this for future uses.")

                self.write(index_name, df)
                self.logger.info(f"Sleeping 5s, before re-query.")
                sleep(5)
                df = self.query_es(index_name, ticker, date)

        # if series is wanted than process it and convert it
        if output_format is "series":
            df = self.convert(df)

        if fill_na & (not df.empty):
            df.fillna(method='ffill', inplace=True)
        return df

    def write(self, index_name: AnyStr, df: pd.DataFrame) -> bool:
        """
        Sends data to ES.
        Data when sent twice leads to duplication in ES. To prevent this, all ticker data is first deleted
        before writing.
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
        self.setup_es_index(index_name=index_name)  # TODO: move this to the init stage
        actions = [
            {
                "_index": index_name,
                "_type": "_doc",
                "_id": 'date' + str(int(node['date'])) + 'Close' + str(node['Close']) + 'ticker' + str(node['ticker']),
                "_source": json.dumps(node)
            }
            for node in df.reset_index().to_dict(orient='records')
        ]
        try:

            # then write the new data
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

    def query_es(self, index_name: AnyStr, ticker: AnyStr, date: List) -> pd.DataFrame:
        """
        Internal method to query the ES. First the full ticker data is queried and once it is received it is filtered
        for the requested dates.

        Args:
            index_name: ES index
            ticker: ticker name
            date: a list of epoch seconds.

        Returns:
            Either
            (1) When the query fails, an empty dataframe with standard columns.
            (2) When the query doesn't fail, a dataframe with values in it including possibly nans (when db doesnt
            have the data for the required dates and ticker)

        Examples:
            Typical situation:
                >>> from portfolio.Database import DB
                >>> db = DB()
                >>> df_ = db.query_es(index_name='time-series',ticker='AMZN',date=[1650844800])
                df
                                 Close ticker
                date
                1650844800  2921.47998   AMZN

            A non-existing ticker:
                >>> df_ = db.query_es(index_name='time-series',ticker='XXADFAFAFAFEAEFAF',date=[1650844800])
                2022-05-03 15:25:32,800 - portfolio.Database - ERROR - 'DataFrame' object has no attribute 'date'
                df
                Empty DataFrame
                Columns: []
                Index: []

            When ticker present but no data for the requested date:
                >>> df_ = db.query_es(index_name='time-series',ticker='AMZN',date=[1650844800])
                df
                                 Close ticker
                date
                1650844800  NaN   AMZN
        """
        # return all data
        s = Search(index=index_name).using(self.client).extra(track_total_hits=True).query(qe.matcher(ticker))
        # res = s.execute()
        # parse the raw results to pandas DF
        try:
            df = pd.DataFrame([hit.to_dict() for hit in s.scan()])
            # TODO: Does the received and requested tickers match?
            ticker_list = df.ticker.unique().tolist()

            # Validations.
            if len(ticker_list) > 1:
                raise Exception(f'Received DF has more than one unique ticker values {ticker_list}')
            if not (df.ticker == ticker).all():
                raise Exception(f"The requested ticker {ticker} does not match to the received ticker {ticker_list[0]}")
            if df.reset_index().duplicated().any():
                raise Exception(f"There are duplicates in the index for ticker {ticker}")

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
                df = pd.DataFrame(index=pd.Index(date, name='date')).join(df, how='left')
                df['ticker'] = ticker
                df.ticker = df.ticker.astype("category")  # need to do this again
                df.columns.name = ticker

            return df
        except NotFoundError as err:
            self.logger.error(err)
        except AttributeError as err:
            self.logger.error(err)

        # empty df with standard columns
        df = pd.DataFrame(columns=['ticker', 'Close'], index=pd.Index([], name='date'))
        df.ticker = df.ticker.astype("category")
        return df

    def delete_ticker(self, ticker: AnyStr, index: AnyStr = 'time-series'):
        # first delete all data

        delete_query = f"""{{"query": {{"match": {{"ticker": "{ticker}"}}}}}}"""
        self.client.delete_by_query(index=index, body=delete_query)
