import yfinance as yf
import pandas as pd
from elasticsearch import Elasticsearch
import uuid
import json
import time
from elasticsearch_dsl import Index
import utils
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = utils.get_logger("downloader")

ES_HOST = "elasticsearch:9200"


def setup_es(index_name, client):
    """Creates indices with INDEX_NAME using elasticsearch CLIENT"""

    logger.info(f"Setting up index {index_name} on {client.info()['cluster_name']}.")
    index = Index(index_name, client)
    index.settings(
        number_of_shards=1,
        number_of_replicas=1, )
    # ignore already exists error
    index.create(ignore=400)


def main():
    """
    Saves historical values fetched from Yahoo Finance to a local Elastic cluster.
    """

    logger.info('Creating an ES client.')
    client_es = Elasticsearch("http://elastic:changeme@" + ES_HOST)

    # create indices if they are not already created
    for ind in ['time-series']:
        if not client_es.indices.exists(ind):
            setup_es(ind, client_es)

    tickers = utils.get_all_tickers()
    logger.info('Found these tickers:\n{}', tickers.to_json())

    for ticker in np.random.choice(tickers, 100):

        logger.info(f"working on ticker {ticker}.")
        # Get ticker's balance sheet from Yahoo Finance
        t = yf.Ticker(ticker)
        df = t.history(period='max', interval='1d')
        # each row is a time point, here a day.
        df = df["Close"]  # take only close. alternative could be open, high, low

        if not df.empty:

            df.index.name = 'date'
            df = df.reset_index()
            # convert unserializable datetime columns to integer
            df['date'] = df['date'].astype('int64') // 1e9
            # add the ticker as a column
            df['ticker'] = ticker
            # add each row to es as a document
            # TODO: This is too serial, for each time point makes an index call. it could be done a bit more parallel.
            for my_dict in df.to_dict(orient='records'):
                # Send JSON to es to INDEX_NAME.
                logger.info('Sending to ES:')
                client_es.index(index='time-series',
                                doc_type='_doc',
                                id=uuid.uuid4(),
                                document=json.dumps(my_dict))

        # Sleep a bit so that Yahoo doesn't black list us
        time.sleep(60)


if __name__ == '__main__':
    main()

