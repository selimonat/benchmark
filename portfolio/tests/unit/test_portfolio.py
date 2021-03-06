from portfolio import utils
from dateutil.parser import parse
from portfolio.Portfolio import Portfolio
from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from portfolio.Ticker import Ticker
from portfolio import Database
import numpy as np

SECONDS_IN_A_DAY = (60 * 60 * 24)
# some example dates.
a_monday = 1649635200
a_tuesday = a_monday + 24 * 60 * 60
a_wednesday = a_tuesday + 24 * 60 * 60
a_thursday = a_wednesday + 24 * 60 * 60
a_friday = a_thursday + 24 * 60 * 60

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
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=a_monday, cost=cost1)
    pos2 = Position(action='buy', quantity=quantity1, ticker='GOOG', date=a_monday, cost=cost1)
    value = 200
    tickers = [Ticker([pos1], value=value, today=a_monday), Ticker([pos2], value=value, today=a_monday)]

    p = Portfolio(tickers)

    r1 = 100 * (value * quantity1 - cost1 * quantity1) / (cost1 * quantity1)
    r2 = 100 * (value * quantity2 - cost2 * quantity2) / (cost2 * quantity2)
    r = (r1 * cost1 * quantity1 + r2 * cost2 * quantity2) / (cost1 * quantity1 + cost2 * quantity2)

    assert p.portfolio_returns.values == r


def test_unbalanced():
    quantity1, cost1 = 1, 100
    quantity2, cost2 = 10, 50
    value = 200
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=a_monday, cost=cost1)
    pos2 = Position(action='buy', quantity=quantity2, ticker='GOOG', date=a_monday, cost=cost2)

    tickers = [Ticker([pos1], value=value, today=a_monday), Ticker([pos2], value=value, today=a_monday)]

    p = Portfolio(tickers)

    r1 = 100 * (value - cost1) / cost1
    r2 = 100 * (value - cost2) / cost2
    r = (r1 * cost1 * quantity1 + r2 * cost2 * quantity2) / (cost1 * quantity1 + cost2 * quantity2)

    assert p.portfolio_returns.values == r


def test_shares_quantities():
    cost = 100  # arbitrary number
    # generate 5 positions of buys and sells for 2 tickers.
    pos1 = Position(action='buy', quantity=10, ticker='FB', date=a_monday, cost=cost)
    pos2 = Position(action='buy', quantity=10, ticker='GOOG', date=a_tuesday, cost=cost)
    pos3 = Position(action='sell', quantity=5, ticker='FB', date=a_wednesday, cost=cost)
    pos4 = Position(action='buy', quantity=10, ticker='FB', date=a_thursday, cost=cost)
    pos5 = Position(action='sell', quantity=5, ticker='GOOG', date=a_thursday, cost=cost)
    #
    p = Portfolio([Ticker([pos1, pos3, pos4], today=a_thursday),
                   Ticker([pos2, pos5], today=a_thursday)
                   ])
    assert np.sum(list(p.current_total_shares_per_ticker.values())) == \
           (np.sum(list(p.current_open_shares_per_ticker.values())) +
            np.sum(list(p.current_closed_shares_per_ticker.values())))


def test_benchmark_returns_must_be_same_when_tested_against_():
    filename = './portfolio/examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    p = Portfolio(pp.tickers, benchmark_symbol='FB')
    assert p.current_portfolio_returns == p.current_benchmark_returns


# def test_fractional_portfolio():
#     filename = './portfolio/examples/portfolio_08.csv'
#     pp = PortfolioParser(filename)
#     p = Portfolio(pp.tickers, benchmark_symbol='FB')
#     a = 1 + 3
#     p.summary
