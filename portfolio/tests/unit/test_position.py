import pytest
from portfolio.Position import Position
from portfolio.utils import last_monday


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