import pytest
from portfolio.Position import Position
from portfolio.utils import last_monday
import numpy as np
from dateutil import parser


def test_invalid_ticker():
    # test with an unlikely ticker name.
    ticker = 'ADSADADSADQWEFQWEFQWEQE'
    start = last_monday()
    with pytest.raises(Exception) as exception:
        Position(action='buy', quantity=1, ticker=ticker, date=start)
    assert f'{ticker} is not a valid ticker, I will not be able to retrieve asset value.' == str(exception.value)


def test_valid_ticker():
    # test with an unlikely ticker name.
    ticker = 'AMZN'
    a_monday = 1649635200
    start = a_monday
    Position(action='buy', quantity=1, ticker=ticker, date=start)
    assert True


def test_valid_quantity():
    # test with an unlikely ticker name.
    ticker = 'AMZN'
    start = last_monday()
    with pytest.raises(Exception) as exception:
        Position(action='buy', quantity=np.nan, ticker=ticker, date=start)
    assert f"{ticker} at {start} cannot have nan quantity." == str(exception.value)


def test_lower_action_strings():
    pos1 = Position(action='BUY', quantity=10, ticker='FB', date=0, cost=10)
    assert pos1.action == 'buy'
    pos1 = Position(action='SELL', quantity=10, ticker='FB', date=0, cost=10)
    assert pos1.action == 'sell'


def test_nan_value():
    """
    the setup uses the .F ticker for Zscaler, which do not have the data for the requested date, although american
    ZS ticker has this data, so the company was public by then. it is a problem of the exchange, not of the ticker.
    In such cases we have to throw an exception.
    """
    ticker='0ZC.F'
    date=int(parser.parse('04-04-2019').timestamp())
    with pytest.raises(Exception) as exception:
        pos1 = Position(action='BUY',
                        quantity=10,
                        ticker=ticker,
                        date=date,
                        )
    assert f"Cannot find the sell/buy value of the position for {ticker} at {date}." == str(exception.value)


def test_a_holiday():
    # there is no data during an holiday. When such a date is requested the Position infers it from the surrounding
    # datapoints. Here we only test that it is not NaN, without further validation.
    ticker = 'GOOG'
    date = 1579478400  # Martin Luther King, Jr.
    pos = Position(action='buy', quantity=1, ticker=ticker, date=date)
    assert pos.cost is not np.nan
