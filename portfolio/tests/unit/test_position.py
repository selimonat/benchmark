import pytest
from portfolio.Position import Position
from portfolio.utils import last_monday
import numpy as np


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
    start = last_monday()
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
