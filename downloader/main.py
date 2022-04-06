import yfinance as yf
import pandas as pd
import time
import utils
from portfolio.Database import DB

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = utils.get_logger("downloader")


def main():
    """
    Saves historical values fetched from Yahoo Finance to a local Elastic cluster.
    """
    db = DB()

    # create indices if they are not already created
    for ind in ['time-series']:
            db.setup_es_index(ind)

    tickers = utils.get_all_tickers()
    logger.info(f'Found these tickers:\n{tickers.to_json()}')

    for ticker in tickers:

        logger.info(f"working on ticker {ticker}.")
        # get ticker from ES.

        # find the maximum value and use the next day as the start argument
        # if exist
        # start  =
        # else
        # start = '1900-01-01'
        t = yf.Ticker(ticker)

        df = t.history(start=start, interval='1d')
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
                    "_type": "_doc",
                    "_id": uuid.uuid4(),
                    "_source": json.dumps(node)
                }
                for node in df.to_dict(orient='records')
            ]
            try:
                response = helpers.bulk(client_es, actions)
                logger.info(f"Bulk sending data to ES, got this {response}.")

            # db.write(df)
            # butun bu asagidaki DB'ye gidiyor.


        # Sleep a bit so that Yahoo doesn't black list us
        logger.info(f"Will wait a bit before the next call")
        time.sleep(20)


if __name__ == '__main__':
    main()
