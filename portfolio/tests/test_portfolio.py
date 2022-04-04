from portfolio import utils
from portfolio.Parser import parse_file
from dateutil.parser import parse
import pandas as pd
from portfolio.Portfolio import Portfolio
from portfolio import Database

SECONDS_IN_A_DAY = (60*60*24)
logger = utils.get_logger(__name__)


db = Database.DB(hostname=None)


def test_return_integer():
    # distance between Start and stop times should return an integer when divided by number of seconds in a day.
    # step size check.

    d = (utils.today() - parse("20200101T000000 UTC").timestamp()) / SECONDS_IN_A_DAY
    logger.info(f"Number of seconds between any time points must be a multiple of number of seconds in a day. Found "
                f"a multiple which is equals to {d}.")
    assert d == round(d)


def test_ticker_column_must_be_categorical():

    df = parse_file(filename='./portfolio/examples/portfolio_02.csv')
    assert isinstance(df['ticker'].dtype, pd.api.types.CategoricalDtype)


def test_action_column_must_be_categorical():

    df = parse_file(filename='./portfolio/examples/portfolio_02.csv')
    assert isinstance(df['action'].dtype, pd.api.types.CategoricalDtype)


def test_no_nan_rows():
    df = parse_file(filename='./portfolio/examples/portfolio_02.csv')
    p = Portfolio(df)
    logger.info(f"There should be no row with only nan values.")
    assert p.table_time_course_asset_price.isna().sum(axis=1).max() < p.table_time_course_asset_price.shape[1]


def test_lot_numbers():
    df = parse_file(filename='./portfolio/examples/portfolio_03.csv')
    p = Portfolio(df)
    logger.info(f"There should be no any row with only nan values.")
    a = p.table_transaction.groupby('ticker').count()['quantity'].to_frame()
    b = p.table_transaction.groupby('ticker').max()['lot'].to_frame()
    df = pd.DataFrame.join(a, b)
    assert df['quantity'].equals(df['lot'])


def test_asset_price():
    logger.info(f"If we randomly select an entry from the asset price time-series, it should match the value in the "
                f"database.")
    df = parse_file(filename='./portfolio/examples/portfolio_03.csv')
    p = Portfolio(df)
    out = p.table_time_course_asset_price.unstack(-1)
    out = out.loc[out.isna() == False].sample(1).reset_index()
    assert db.read(out['level_0'].values[0], out['time'].to_list()) == out[0]

#  TODO: asset return at purchase date must be 100%
#  TODO: asset time course table must have as many columns as the number of shares.
#  TODO: Transaction table must have indices from 0 to the shape[0] of the table.
#  TODO: Two times the same ticker must result in two different columns in the time-course table.
#  TODO: When same ticker added twice the asset price must be the mean of them