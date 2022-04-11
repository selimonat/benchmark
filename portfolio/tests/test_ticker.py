from portfolio.Ticker import ShareCounter, Ticker
from portfolio.Parser import PortfolioParser
from portfolio.Plotter import console_plot


def test_ShareCounter_basics():
    count = ShareCounter()
    assert len(count.shares) == 0
    count.add_new()
    assert len(count.shares) == 1
    assert count.shares[-1] == 1
    assert count.shares[0] == 1
    count.add_new()
    count.add_new()
    count.add_new()
    assert len(count.shares) == 4
    assert count.shares[-1] == 4
    assert count.shares[0] == 1
    count.remove_old()
    assert len(count.shares) == 3
    assert count.shares[-1] == 4
    assert count.shares[0] == 2
    count.remove_old()
    assert len(count.shares) == 2
    assert count.shares[-1] == 4
    assert count.shares[0] == 3


def test_add_position():
    # test if number of added positions is in line with the number of to-be-added positions.
    # portfoloi 3 has only open positions as set of actions.
    filename = '../examples/portfolio_03.csv'
    pp = PortfolioParser(filename)

    positions = pp.grouped_positions
    ticker = list(positions.keys())[-1]

    t = Ticker(positions[ticker])
    # shares from transaction table
    total_shares = sum([pos.quantity for pos in pp.positions if pos.ticker == ticker])

    assert t.investment.shape[1] == total_shares == t.open_shares


def test_remove_positions():
    # test if number of added positions is in line with the number of to-be-added positions.
    # use a set of transactions that also include closing position
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    positions = pp.grouped_positions
    ticker = list(positions.keys())[-1]
    t = Ticker(positions[ticker])

    # shares from transaction table
    total_shares = sum([pos.quantity for pos in pp.positions if pos.ticker == ticker])
    # shares from the ticker object
    open_shares = t.investment.iloc[-1, :].notna().sum()

    assert open_shares == total_shares == t.open_shares
