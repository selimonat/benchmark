from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
import pytest


def test_non_homogenous_ticker_list():
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    # take more or less randomly a ticker
    with pytest.raises(Exception) as exception:
        Ticker(pp.positions)
    assert "There are different tickers in the positions list..." == str(exception.value)


def test_share_numbers_for_a_ticker():
    # compare number of shares from the parsed table with those from the ticker data frames.
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    # take more or less randomly a ticker
    ticker = pp.tickers[-1]
    # parsed export file
    parsed_df = pp.grouped_positions_df[ticker]

    #
    t = Ticker(pp.grouped_positions[ticker])

    buys = parsed_df.loc[parsed_df.action == 'buy', 'quantity'].sum()
    sells = parsed_df.loc[parsed_df.action == 'sell', 'quantity'].sum()
    assert t.total_shares == buys
    assert t.current_open_shares == buys + sells
    assert t.current_sold_shares == -sells
