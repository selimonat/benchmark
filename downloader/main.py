import yfinance as yf
import pandas as pd
import time
import utils
from portfolio.Database import DB
import datetime

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
    if tickers is None:
        raise Exception(f"We can''t continue without a list of tickers {tickers}.")

    logger.info(f'Found tickers {tickers.shape}:\n{tickers.to_json()}')

    for ticker in tickers:

        logger.info(f"==================Working on ticker {ticker}==================")
        # get ticker from ES.
        df_db = db.read(ticker, output_format='series')
        # find the maximum value and use the next day as the start argument
        if not df_db.empty:
            start_minus_one = df_db.index.max()
            logger.info(f"Received a DF of size {df_db.shape[0]} from the DB.")
            logger.info(f"Last found date in the DB is {datetime.datetime.fromtimestamp(start_minus_one)}.")
            start = start_minus_one + 24 * 60 * 60
            start = datetime.datetime.fromtimestamp(start)
            logger.info(f"The first missing date is {start}.")
        else:
            start = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')

        t = yf.Ticker(ticker)

        df = t.history(start=start.strftime('%Y-%m-%d'), interval='1d')
        # make it fucking sure that start is start and that no previous days are included.
        df = df.loc[df.index > start]
        # take only close. alternative could be open, high, low
        df = df["Close"]
        if not df.empty:
            logger.info(f"Got a DF of size {df.shape[0]}, the first date is {df.index.min()} and the last one is"
                        f" {df.index.max()}")
            df.index.name = 'date'
            df = df.reset_index()
            # convert unserializable datetime columns to integer
            df['date'] = df['date'].astype('int64') // 1e9
            # add the ticker as a column
            df['ticker'] = ticker
            df = df.set_index('date')

            if db.write(ind, df):
                logger.info(f'Success... Wrote {df.shape[0]} new rows for {ticker}.')
                logger.info(f"Will wait a bit before the next call")
                time.sleep(5)
            else:
                logger.warning(f'Something went wrong while writing {ticker} to db...')
                # Sleep a bit so that Yahoo doesn't black list us
        else:
            logger.warning(f'Returned/Filtered df for {ticker} is empty.')


if __name__ == '__main__':
    main()
