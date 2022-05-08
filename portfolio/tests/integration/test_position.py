from dateutil import parser
import pytest
from portfolio.Position import Position


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