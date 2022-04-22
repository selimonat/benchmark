from portfolio import utils
from portfolio.Parser import PortfolioParser
from dateutil.parser import parse
import pandas as pd
from portfolio.Portfolio import Portfolio
from portfolio.Position import Position
from portfolio.Ticker import Ticker
from portfolio import Database
import numpy as np

SECONDS_IN_A_DAY = (60 * 60 * 24)
logger = utils.get_logger(__name__)

db = Database.DB()


def test_return_integer():
    # distance between Start and stop times should return an integer when divided by number of seconds in a day.
    # step size check.

    d = (utils.today() - parse("20200101T000000 UTC").timestamp()) / SECONDS_IN_A_DAY
    logger.info(f"Number of seconds between any time points must be a multiple of number of seconds in a day. Found "
                f"a multiple which is equals to {d}.")
    assert d == round(d)


def test_balanced():
    quantity1, cost1 = 1, 100
    quantity2, cost2 = 1, 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    pos2 = Position(action='buy', quantity=quantity1, ticker='GOOG', date=utils.today(), cost=cost1)
    value = 200
    tickers = [Ticker([pos1], value=value), Ticker([pos2], value=value)]

    p = Portfolio(tickers)

    r1 = 100 * (value * quantity1 - cost1 * quantity1) / (cost1 * quantity1)
    r2 = 100 * (value * quantity2 - cost2 * quantity2) / (cost2 * quantity2)
    r = (r1 * cost1 * quantity1 + r2 * cost2 * quantity2) / (cost1 * quantity1 + cost2 * quantity2)

    assert p.returns.values == r


def test_unbalanced():
    quantity1, cost1 = 1, 100
    quantity2, cost2 = 10, 50
    value = 200
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    pos2 = Position(action='buy', quantity=quantity2, ticker='GOOG', date=utils.today(), cost=cost2)

    tickers = [Ticker([pos1], value=value), Ticker([pos2], value=value)]

    p = Portfolio(tickers)

    r1 = 100 * (value - cost1) / cost1
    r2 = 100 * (value - cost2) / cost2
    r = (r1 * cost1 * quantity1 + r2 * cost2 * quantity2) / (cost1 * quantity1 + cost2 * quantity2)

    assert p.returns.values == r


def test_shares_quantities():
    cost = 100  # arbitrary number
    # generate 5 positions of buys and sells for 2 tickers.
    pos1 = Position(action='buy', quantity=10, ticker='FB', date=utils.today() - 24 * 60 * 60 * 149, cost=cost)
    pos2 = Position(action='buy', quantity=10, ticker='GOOG', date=utils.today() - 24 * 60 * 60 * 100, cost=cost)
    pos3 = Position(action='sell', quantity=5, ticker='FB', date=utils.today() - 24 * 60 * 60 * 50, cost=cost)
    pos4 = Position(action='buy', quantity=10, ticker='FB', date=utils.today(), cost=cost)
    pos5 = Position(action='sell', quantity=5, ticker='GOOG', date=utils.today(), cost=cost)
    #
    p = Portfolio([Ticker([pos1, pos3, pos4]),
                   Ticker([pos2, pos5])
                   ])
    assert np.sum(list(p.total_shares.values())) == \
           (np.sum(list(p.open_shares.values())) +
            np.sum(list(p.closed_shares.values())))
