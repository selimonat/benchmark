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
    for filename in ['../examples/portfolio_03.csv', '../examples/portfolio_05.csv']:
        pp = PortfolioParser(filename)
        # take more or less randomly a ticker
        ticker = pp.ticker_names[-1]
        # parsed export file
        parsed_df = pp.grouped_positions_df[ticker]
        #
        t = Ticker(pp.grouped_positions[ticker])
        # ground truth
        buys = parsed_df.loc[parsed_df.action == 'buy', 'quantity'].sum()
        sells = parsed_df.loc[parsed_df.action == 'sell', 'quantity'].sum()

        assert t.current_open_shares == (buys + sells)  # number of shares that are currently open
        assert t.current_total_shares == buys  # number of shares that were ever bought
        assert t.current_closed_shares == -sells  # number of shares that were ever sold


def test_parameters_for_bought_one_share_one_lot():
    # Create a Position object and pass it directly to Ticker for testing.
    quantity1 = 10
    cost1 = 100
    value = 110

    pos = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today(), cost=cost1)
    t = Ticker([pos], value=value)

    assert t.current_open_shares == quantity1
    assert t.current_closed_shares == 0
    assert t.current_invested == cost1 * quantity1
    assert t.current_cost == cost1 * quantity1
    assert t.current_open_shares == quantity1
    assert t.current_closed_shares == 0
    assert t.current_profit_loss == 0
    assert t.current_unrealized_gain == (value - cost1) * quantity1
    assert t.current_ticker_value == value


def test_parameters_for_bought_one_share_two_lots():
    quantity1 = 10
    cost1 = 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60, cost=cost1)

    quantity2 = 5
    cost2 = 150
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today(), cost=cost2)

    value = 200
    t = Ticker([pos1, pos2], value=value, clean_weekends=False)

    assert t.current_open_shares == quantity1 + quantity2
    assert t.current_closed_shares == 0
    assert t.current_invested == cost1 * quantity1 + cost2 * quantity2
    assert t.current_cost == cost1 * quantity1 + cost2 * quantity2
    assert t.current_open_shares == quantity1 + quantity2
    assert t.current_closed_shares == 0
    assert t.current_profit_loss == 0
    assert t.current_unrealized_gain == value * (quantity2 + quantity1) - (cost1 * quantity1 + cost2 * quantity2)
    assert t.current_ticker_value == value


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

    assert t.current_open_shares == quantity1 + quantity2
    assert t.current_closed_shares == 0
    assert t.current_invested == cost1 * quantity1 + cost2 * quantity2
    assert t.current_cost == cost1 * quantity1 + cost2 * quantity2
    assert t.current_open_shares == quantity1 + quantity2
    assert t.current_closed_shares == 0
    assert t.current_profit_loss == 0
    assert t.current_unrealized_gain == current_value * (quantity2 + quantity1) - (
            cost1 * quantity1 + cost2 * quantity2)
    assert t.current_ticker_value == current_value


def test_parameters_for_buy_two_sell_one_lot():
    # first batch bought 10 at 110
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60 * 2, cost=cost1)
    # Second batch, bought 1 at 100
    quantity2 = 1
    cost2 = 100
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today() - 24 * 60 * 60, cost=cost2)
    # Sold 5
    quantity3 = 5

    pos3 = Position(action='sell', quantity=quantity3, ticker='FB', date=utils.today())
    # today the value is 110.
    current_value = 110
    t = Ticker([pos1, pos2, pos3], value=[cost1, cost2, current_value], clean_weekends=False)

    # number of shares I have today
    quantity = quantity1 - quantity3 + quantity2
    # their overall value
    value = current_value * quantity
    # how much the current portfolio cost us, this is the invested amount.
    # due to the FIFO principle we sold 5 shares from the first lot, which corresponds to quantity1-quantity3
    cost = cost1 * (quantity1 - quantity3) + cost2 * quantity2

    assert t.current_open_shares == quantity
    assert t.current_closed_shares == quantity3
    assert t.current_invested == cost
    assert t.current_cost == cost
    assert t.current_profit_loss == 0
    assert t.current_unrealized_gain == current_value * (quantity2 + quantity1) - (
            cost1 * quantity1 + cost2 * quantity2)
    assert t.current_ticker_value == current_value
    assert t.current_value == quantity * current_value


def test_1():
    # Simple opening of a position
    # Tests the mat_* dataframe and tc_* properties.
    quantity1 = 2
    cost1 = 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60 * 1, cost=cost1)
    current_value = 110
    t = Ticker([pos1], value=[cost1, current_value], clean_weekends=False)

    assert (t.mat_investment.values == np.vstack([[cost1, cost1], [cost1, cost1]])).all()
    assert (t.mat_value.values == np.vstack([[100, 100], [110, 110]])).all()
    assert (t.mat_profit_loss.values == np.vstack([[0, 0], [0, 0]])).all()

    assert (t.tc_cost.values == [200, 200]).any()
    assert (t.tc_value.values == [200, 220]).any()
    assert (t.tc_profit_loss.values == [0, 0]).any()
    assert (t.tc_unrealized_gain.values == [0, 20]).any()
    assert (t.tc_returns.values == [0, 20 / 200 * 100]).any()
    assert (t.tc_average_cost_per_share.values == [200 / 2, 200 / 2]).any()


def test_2():
    # Open two positions.
    # Tests the mat_* dataframe and tc_* properties.
    quantity1 = 2
    cost1 = 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60 * 2, cost=cost1)

    quantity2 = 1
    cost2 = 120
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today() - 24 * 60 * 60 * 1, cost=cost2)

    current_value = 150
    t = Ticker([pos1, pos2], value=[cost1, cost2, current_value], clean_weekends=False)

    assert np.array_equal(t.mat_investment.values, np.vstack([[100, 100, np.nan], [100, 100, 120], [100, 100, 120]]),
                          equal_nan=True)
    assert np.array_equal(t.mat_value.values, np.vstack([[100, 100, np.nan], [120, 120, 120], [150, 150, 150]]),
                          equal_nan=True)
    assert np.array_equal(t.mat_profit_loss.values, np.vstack([[0, 0, np.nan], [0, 0, 0], [0, 0, 0]]),
                          equal_nan=True)
    assert (t.tc_cost.values == [200, 320, 320]).any()
    assert (t.tc_value.values == [200, 360, 450]).any()
    assert (t.tc_profit_loss.values == [0, 0, 0]).any()
    assert (t.tc_unrealized_gain.values == [0, 40, 130]).any()
    assert (t.tc_returns.values == [0, 40 / 320 * 100, 130 / 320 * 100]).any()
    assert (t.tc_average_cost_per_share.values == [200 / 2, 320 / 3, 320 / 3]).any()


def test_3():
    # opening two positions, closing one. and opening another one. Tests the mat_* dataframe and tc_* properties.
    quantity1 = 2
    cost1 = 100
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60 * 4, cost=cost1)

    quantity2 = 1
    cost2 = 120
    pos2 = Position(action='buy', quantity=quantity2, ticker='FB', date=utils.today() - 24 * 60 * 60 * 3, cost=cost2)

    quantity3 = 2
    cost3 = 150  # unused for position but used as value in Ticker
    pos3 = Position(action='sell', quantity=quantity3, ticker='FB', date=utils.today() - 24 * 60 * 60 * 2)

    quantity4 = 1
    cost4 = 150
    pos4 = Position(action='buy', quantity=quantity4, ticker='FB', date=utils.today() - 24 * 60 * 60, cost=cost4)

    current_value = 150
    t = Ticker([pos1, pos2, pos3, pos4], value=[cost1, cost2, cost3, cost4, current_value], clean_weekends=False)

    assert np.array_equal(t.mat_investment.values, np.vstack([[100, 100, np.nan, np.nan],
                                                              [100, 100, 120, np.nan],
                                                              [np.nan, np.nan, 120, np.nan],
                                                              [np.nan, np.nan, 120, 150],
                                                              [np.nan, np.nan, 120, 150],
                                                              ]),
                          equal_nan=True)

    assert np.array_equal(t.mat_value.values, np.vstack([[100, 100, np.nan, np.nan],
                                                         [120, 120, 120, np.nan],
                                                         [np.nan, np.nan, 150, np.nan],
                                                         [np.nan, np.nan, 150, 150],
                                                         [np.nan, np.nan, 150, 150],
                                                         ]),
                          equal_nan=True)

    assert np.array_equal(t.mat_profit_loss.values, np.vstack([[0, 0, np.nan, np.nan],
                                                               [0, 0, 0, np.nan],
                                                               [50, 50, 0, np.nan],
                                                               [50, 50, 0, 0],
                                                               [50, 50, 0, 0],
                                                               ]),
                          equal_nan=True)

    assert (t.tc_cost.values == [200, 320, 120, 270, 270]).any()
    assert (t.tc_value.values == [200, 360, 150, 300, 300]).any()
    assert (t.tc_profit_loss.values == [0, 0, 300, 300, 300]).any()
    assert (t.tc_unrealized_gain.values == [0, 40, 30, 30, 30]).any()
    assert (t.tc_returns.values == [0, 40 / 320 * 100, 30 / 150 * 100, 30 / 300 * 100, 30 / 300 * 100]).any()
    assert (t.tc_average_cost_per_share.values == [200 / 2, 320 / 3, 120 / 1, 270 / 2, 270 / 2]).any()


def test_not_enough_shares_to_sell():
    pos = Position(action='sell', quantity=3, ticker='FB', date=utils.today())
    with pytest.raises(Exception) as exception:
        Ticker([pos])
    assert 'You do not have enough shares to sell.' == str(exception.value)


def test_timeline_should_not_have_any_weekends():
    quantity1 = 10
    cost1 = 110
    pos1 = Position(action='buy', quantity=quantity1, ticker='FB', date=utils.today() - 24 * 60 * 60 * 100, cost=cost1)
    ticker = Ticker([pos1])

    assert np.all([datetime.datetime.fromtimestamp(t).weekday() <= 4 for t in ticker.time_line])


def test_return_at_purchase_date():
    #  The return must be 0% at the purchase date.

    date = 1637884800  # This is not a weekend, but the test fails when date is weekend (see test below)
    pos1 = Position(action='buy', quantity=1, ticker='FB', date=date)
    t = Ticker([pos1])
    assert t.tc_returns[date] == 0


def test_return_at_purchase_date_weekend():
    #  The return must be 0% at the purchase date.

    # open a FB position 149 days before today
    # this is interesting if 150 days, then it doesn't work, it is probably a holiday or so.
    date = 1637971200  # the test may fail if the day is a weekend day.
    pos1 = Position(action='buy', quantity=1, ticker='FB', date=date)
    t = Ticker([pos1])
    assert t.tc_returns[date] == 0


def test_unrealized_gains():
    quantity = 10
    cost = 100
    value = 200
    pos1 = Position(action='buy', quantity=quantity, ticker='FB', date=utils.today() - 24 * 60 * 60 * 100, cost=cost)
    ticker = Ticker([pos1], value=value)
    assert ticker.tc_unrealized_gain.iloc[-1] == (value - cost) * quantity
