import logging
import os
from typing import Union, AnyStr
import datetime
from urllib.error import URLError
import yfinance as yf

import pandas as pd


def get_all_tickers() -> Union[pd.Series, None]:
    """
    Gets all tickers listed in the Nasdaq exchange from their public ftp server.

    Returns:
        (None or Series): A pandas series of Nasdaq listed tickers.
    """

    # an alternative ticker list, maybe be merge this with the one above.
    # url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'
    cached_file = './benchmark/downloader/tickers/nasdaq.csv'
    url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
    logger = get_logger('get_all_tickers')
    try:
        logger.info('Connecting to Nasdaq servers to fetch ticker list.')
        df = pd.read_csv(url, sep='|', skipfooter=1)
        # df can still be returned empty
        if df.empty:
            logger.info("Returned dataframe is empty.")
            raise ValueError('Returned dataframe is empty')

        logger.info(f"Found DataFrame: {df.head(10)}")
        logger.info(f'Caching the list to local disk as {cached_file}.')
        df.to_csv(cached_file)
        return df['Symbol']

    except (URLError, ValueError) as error:
        logging.error(error)
        logging.error(f'URL {url}')
        logging.warning("Attempt to load cached tickers.")
        if os.path.exists(cached_file):
            df = pd.read_csv(cached_file)
            logging.warning(f"Seemed to work, found df with {df.shape[0]} rows.")
            return df['Symbol']
        else:
            logging.warning('Cached file doesnt exist.')
            return None


def get_logger(name):
    # create logger
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.DEBUG)
    # create formatter
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    # create console handler and set level to debug and add formatter to ch
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    # add ch to logger
    logger_.addHandler(ch)
    # do the same with a file handler.
    if not os.path.exists('log'):
        os.makedirs('log')
    f_handler = logging.FileHandler(os.path.join('log', f'{name}.log'))
    f_handler.setLevel(logging.DEBUG)
    f_handler.setFormatter(formatter)
    logger_.addHandler(f_handler)
    return logger_


def yf_call(ticker: AnyStr,
            start: datetime = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')) -> pd.DataFrame:
    """
    Makes a YF call and returns tickers data.
    Args:
        ticker: ticker symbol
        start: (datetime) when not given complete dataset is returned.
    Returns:
        dataframe:  a standard DF with Close, ticker columns indexed on date.
    """
    logger = get_logger(__name__)

    logger.info(f"Making a direct YF call for ticker: {ticker} and start_date: {start}")
    t = yf.Ticker(ticker)
    df = t.history(start=start.strftime('%Y-%m-%d'), interval='1d')
    # make it fucking sure that start is start and that no previous days are included.

    df = df.loc[df.index > start]

    # Adapt YF output to benchmark standards.
    df = df["Close"]     # take only close. alternative could be open, high, low
    if not df.empty:
        df.index.name = 'date'
        df = df.reset_index()
        # add the ticker as a column
        df['ticker'] = ticker
        # datatypes, convert unserializable datetime columns to integer
        df['date'] = df['date'].astype('int64') // 1e9
        df.ticker = df.ticker.astype("category")
        df.Close = df.Close.astype(float)
        df = df.set_index('date')

    return df
