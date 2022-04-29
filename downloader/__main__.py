from typing import AnyStr, Union
import pandas as pd
import time
from downloader import utils
from portfolio.Database import DB
import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = utils.get_logger("downloader")


def updater(tickers: Union[AnyStr, None] = None) -> None:
    """
    Saves historical values fetched from Yahoo Finance (YF) to local Elasticsearch cluster.
    Multiple runs updates the missing time points in the ES.
    Args:
        tickers (str): If present (Example: 'FB') then fetches data for that specific ticker. Otherwise starts a
        whole update cycle, running across all tickers returned by `utils.get_all_tickers()`.
    """
    db = DB()

    # create indices if they are not already created
    for ind in ['time-series']:
        db.setup_es_index(ind)

    if tickers is None:  # fetch a list of tickers if not given
        logger.info("Fetching tickers from internet.")
        tickers = utils.get_all_tickers()
        if tickers is None:
            raise Exception(f"We can''t continue without a list of tickers {tickers}.")
        logger.info(f'Found tickers {tickers.shape}:\n{tickers.to_json()}')
    else:
        tickers = pd.Series(tickers, name='Symbol')  # put it to the same format returned by utils.get_all_tickers().

    for ticker in tickers:

        logger.info(f"==================Working on ticker {ticker}==================")
        # get ticker from ES.
        df_db = db.read(ticker, output_format='series')
        # find the latest available date and use its following day as the start argument for the YF call.
        start = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
        if not df_db.empty:
            start_minus_one = df_db.index.max()
            logger.info(f"Received a DF of size {df_db.shape[0]} from the DB.")
            logger.info(f"Last found date in the DB is {datetime.datetime.fromtimestamp(start_minus_one)}.")
            start = start_minus_one + 24 * 60 * 60
            start = datetime.datetime.fromtimestamp(start)
            logger.info(f"The first missing date is {start}.")
        # get a standardized DF from yf
        df = utils.yf_call(ticker, start)

        if not df.empty:
            logger.info(f"Got a DF of size {df.shape[0]}, the first date is {df.index.min()} and the last one is"
                        f" {df.index.max()}")
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
    _ = 'BAS.F'
    updater(_)
