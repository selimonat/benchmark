from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from portfolio import utils
import pytest
from portfolio.Plotter import console_plot
import numpy as np
import pandas as pd
from math import isclose


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
    ticker = pp.ticker_names[-1]
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
    quantity = 100
    cost = 100
    current_value = 110
    pos1 = Position(action='buy',
                    quantity=quantity,
                    ticker='FB',
                    date=utils.today(),
                    cost=cost)
    t = Ticker([pos1], value=current_value)
    assert isclose(t.returns.values, 100 * (current_value - cost) / cost)
    assert t.total_shares == quantity
    assert t.profit_loss.sum(axis=1).values == 0
    assert t.unrealized_gain.values == (current_value - cost) * 100
    assert t.total_invested.values == quantity * cost
    assert t.current_open_shares == quantity
    assert t.current_sold_shares == 0


def test_parameters_for_bought_one_share_two_lots():
    quantity1 = 1
    cost1 = 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    quantity2 = 1
    cost2 = 100
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today(), cost=cost2)
    current_value = 110
    t = Ticker([pos1, pos2], value=[current_value])

    cost = (cost1 + cost2) / 2
    quantity = quantity1 + quantity2
    assert isclose(t.returns.values, 100 * (current_value - cost) / cost)
    assert t.total_shares == quantity1 + quantity2
    assert t.profit_loss.sum(axis=1).values == 0
    assert t.unrealized_gain.values == (current_value - cost) * quantity
    assert t.total_invested.values == quantity * cost
    assert t.current_open_shares == quantity
    assert t.current_sold_shares == 0


def test_parameters_for_bought_different_shares_two_lots():
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    quantity2 = 1
    cost2 = 100
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today(), cost=cost2)
    current_value = 110
    t = Ticker([pos1, pos2], value=[current_value])

    quantity = quantity1 + quantity2
    cost = cost1 * quantity1 / quantity + cost2 * quantity2 / quantity

    assert isclose(t.returns.values, 100 * (current_value - cost) / cost)
    assert t.total_shares == quantity1 + quantity2
    assert t.profit_loss.sum(axis=1).values == 0
    assert t.unrealized_gain.values == (current_value*quantity - (cost1*quantity1 + cost2*quantity2))
    assert t.total_invested.values == (cost1*quantity1 + cost2*quantity2)
    assert t.current_open_shares == quantity
    assert t.current_sold_shares == 0


def test_parameters_for_buy_two_sell_one_lot():
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    quantity2 = 1
    cost2 = 100
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today(), cost=cost2)
    quantity3 = 5
    cost3 = 110
    pos3 = Position(action='sell', quantity=quantity3, ticker='FB', date=utils.today(), cost=cost3)
    current_value = 110
    t = Ticker([pos1, pos2, pos3], value=[current_value])

    quantity = quantity1 - quantity3 + quantity2
    cost = cost1*(quantity1-quantity3) + cost2*quantity2
    value = current_value*quantity

    assert isclose(t.returns.values, (value-cost)/cost*100)
    assert t.total_shares == quantity1 + quantity2
    assert t.profit_loss.sum(axis=1).values == current_value*quantity3
    assert t.unrealized_gain.values == current_value*quantity - (cost1*5+cost2)
    assert t.total_invested.values == (cost1*(quantity1-quantity3) + cost2*quantity2)
    assert t.current_open_shares == quantity
    assert t.current_sold_shares == quantity3

#  TODO: asset return at purchase date must be 100%