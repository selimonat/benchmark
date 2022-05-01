from time import time
from math import floor
import logging
import os
import datetime
import yfinance as yf
from typing import AnyStr
from functools import lru_cache


def today():
    """
    Epoch second representation of today taking local 00:00 as the reference point.
    :return: integer, epoch timestamp
    """
    return floor((time() / (60 * 60 * 24))) * (60 * 60 * 24)


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


def ensure_iterable(obj):
    """
    Ensures OBJ is iterables by wrapping it inside a list when needed.
    :param obj:
    :return:
    """
    if type(obj) != list:
        obj = [obj]
    return obj


def group_rank_id(df, grouper, sorter):
    # function to apply to each group
    def group_fun(x): return x[sorter].reset_index(drop=True).reset_index().rename(columns={'index': 'rank'})
    # apply and merge to itself
    out = df.groupby(grouper).apply(group_fun).reset_index(drop=True)
    return df.merge(out, on=sorter)


def parse_epoch(time_):
    """
    :param time_: epoch timestamp
    :return: iso 8601 formatted datetime str that cbpro API is expecting.
    """
    return datetime.datetime.fromtimestamp(time_, tz=datetime.timezone.utc)


def is_weekend(time_):
    """
    Weekend detector.
    Args:
        time_: Epoch seconds
    Returns:
        (boolean) True if corresponding day is a weekend.
    """
    return parse_epoch(time_).weekday() > 4


def last_monday():
    """Returns last weeks monday as epoch second"""
    today_ = today()
    return today_ - (7 + datetime.timedelta(days=datetime.date.today().weekday()).days) * 24*60*60


@lru_cache(maxsize=None)
def is_valid_ticker(ticker: AnyStr) -> bool:
    """
    Validates ticker against a direct YF call. If ticker has time-series data for last week's monday, it is considered a
    valid ticker.
    Args:
        ticker:
    Returns:
    """
    start = parse_epoch(last_monday()).strftime('%Y-%m-%d')
    df = yf.Ticker(ticker).history(start=start, interval='1d')
    if not df.empty:
        return True
    else:
        return False
