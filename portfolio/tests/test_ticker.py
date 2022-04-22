from portfolio.Ticker import Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from portfolio import utils
import pytest
import numpy as np
from math import isclose
import datetime


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
    # first batch bought 10 at 110
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    # Second batch, bought 1 at 100
    quantity2 = 1
    cost2 = 100
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today(), cost=cost2)
    # Sold 5
    quantity3 = 5
    cost3 = 110
    pos3 = Position(action='sell', quantity=quantity3, ticker='FB', date=utils.today(), cost=cost3)
    # today the value is 110.
    current_value = 110
    t = Ticker([pos1, pos2, pos3], value=[current_value])

    # number of shares I have today
    quantity = quantity1 - quantity3 + quantity2
    # their overall value
    value = current_value*quantity
    # how much the current portfolio cost us, this is the invested amount.
    # due to the FIFO principle we sold 5 shares from the first lot, which corresponds to quantity1-quantity3
    cost = cost1*(quantity1-quantity3) + cost2*quantity2

    assert isclose(t.returns.values, (value-cost)/cost*100)
    assert t.total_shares == quantity1 + quantity2
    assert t.current_sold_shares == quantity3
    assert t.current_open_shares == quantity
    assert t.profit_loss.sum(axis=1).values == current_value*quantity3 - cost1*quantity3
    assert t.unrealized_gain.values == current_value*quantity - (cost1*5+cost2)
    assert t.total_invested.values == (cost1*(quantity1-quantity3) + cost2*quantity2)
    assert t.current_open_shares == quantity
    assert t.current_sold_shares == quantity3


def test_not_enough_shares_to_sell():
    pos = Position(action='sell', quantity=3, ticker='FB', date=utils.today())
    with pytest.raises(Exception) as exception:
        Ticker([pos])
    assert 'You do not have enough shares to sell.' == str(exception.value)


def test_timeline_should_not_have_any_weekends():
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today()-24*60*60*100, cost=cost1)
    ticker = Ticker([pos1])

    assert np.all([datetime.datetime.fromtimestamp(t).weekday() <= 4 for t in ticker.time_line])


def test_return_at_purchase_date():
    #  The return must be 0% at the purchase date.

    # open a FB position 149 days before today
    # this is interesting if 150 days, then it doesn't work, it is probably a holiday or so.
    date = utils.today()-24*60*60*149
    pos1 = Position(action='buy', quantity=1, ticker='FB', date=date)
    t = Ticker([pos1])
    assert t.returns[date] == 0


def test_unrealized_gains():
    quantity = 10
    cost = 100
    value = 200
    pos1 = Position(action='buy', quantity=quantity, ticker='FB', date=utils.today()-24*60*60*100, cost=cost)
    ticker = Ticker([pos1], value=value)
    assert ticker.unrealized_gain.iloc[-1] == (value-cost) * quantity


def test_profit_loss():
    quantity_to_buy = 10
    quantity_to_sell = 5
    bought_at = 100
    value = sold_at = 200
    pos1 = Position(action='buy', quantity=quantity_to_buy, ticker='FB', date=utils.today()-24*60*60*100,
                    cost=bought_at)
    pos2 = Position(action='sell', quantity=quantity_to_sell, ticker='FB', date=utils.today()-24*60*60*50)
    ticker = Ticker([pos1, pos2], value=value)

    # bought 10 shares at 100$, spent 1000$
    # at some point sold 5 of them for 150 for 150*5=750$.
    # this corresponds to 50*5 = 250$ profit.

    assert ticker.profit_loss.iloc[-1].sum() == quantity_to_sell*(sold_at-bought_at)
    assert ticker.unrealized_gain.iloc[-1].sum() == (quantity_to_buy-quantity_to_sell) * (value - bought_at)
