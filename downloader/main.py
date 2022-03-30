import yfinance as yf
import pandas as pd
from elasticsearch import Elasticsearch, helpers
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

ES_HOST = "localhost:9200"


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
        if not client_es.indices.exists(index=ind):
            setup_es(ind, client_es)

    tickers = utils.get_all_tickers()
    logger.info(f'Found these tickers:\n{tickers.to_json()}')

    for ticker in tickers:

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
            # TODO: in the next calls we will need to add only those values that are not in the elastic search alrady.
            # TODO: Create a function returning yield to replace the dict actions below.
            actions = [
                {
                    "_index": "time-series",
                    # "_type": "_doc",
                    "_id": uuid.uuid4(),
                    "_document": json.dumps(node)
                }
                for node in df.to_dict(orient='records')
            ]
            try:
                response = helpers.bulk(client_es, actions)
                logger.info(f"Bulk sending data to ES, got this {response}.")

            except:
                logger.error(f"Bulk sending data to ES failed..")

        # Sleep a bit so that Yahoo doesn't black list us
        logger.info(f"Will wait a bit before the next call")
        time.sleep(20)


if __name__ == '__main__':
    main()
