import pandas as pd
import logging
import os


def get_all_tickers():
    """
    Gets all tickers listed in the Nasdaq exchange from their public ftp server.
    :return: symbol list as a pandas series.
    """
    # https://www.nasdaqtrader.com/Trader.aspx?id=SymbolDirDefs
    url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
    # url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'
    df = pd.read_csv(url, sep='|', skipfooter=1)
    return df['Symbol']


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
