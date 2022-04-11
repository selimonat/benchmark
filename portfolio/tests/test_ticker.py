from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from portfolio import utils
import pytest
from portfolio.Plotter import console_plot
import numpy as np
import pandas as pd


def test_non_homogenous_ticker_list():
    # Ticker should throw an exception when passed a list with more than one ticker name.
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    # take more or less randomly a ticker
    with pytest.raises(Exception) as exception:
        Ticker(pp.positions)
    assert "There are different tickers in the positions list..." == str(exception.value)


def test_no_input():
    # take more or less randomly a ticker
    with pytest.raises(Exception) as exception:
        Ticker([])
    assert "No positions are given..." == str(exception.value)


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


def test_parameters_for_bought_one_share_one_lot():
    pos = Position(action='buy', quantity=1, ticker='FB', date=utils.today(), cost=100)
    t = Ticker([pos], value=100)
    assert t.returns.values == 0
    assert t.total_shares == 1
    assert t.profit_loss.values == 0
    assert t.unrealized_gain.values == 0
    assert t.total_invested.values == 100
    assert t.current_open_shares == 1
    assert t.current_sold_shares == 0


def test_parameters_for_bought_n_shares_one_lot():
    pos = Position(action='buy', quantity=100, ticker='FB', date=utils.today(), cost=100)
    t = Ticker([pos], value=100)
    assert t.returns.values == 0
    assert t.total_shares == 100
    assert t.profit_loss.sum(axis=1).values == 0
    assert t.unrealized_gain.values == 0
    assert t.total_invested.values == 100 * 100
    assert t.current_open_shares == 100
    assert t.current_sold_shares == 0
