from portfolio import utils
from portfolio.Parser import parse_file
from dateutil.parser import parse
import pandas as pd
from portfolio.Portfolio import Portfolio, Position
from portfolio import Database
import numpy as np

SECONDS_IN_A_DAY = (60 * 60 * 24)
logger = utils.get_logger(__name__)

db = Database.DB()


def test_return_integer():
    # distance between Start and stop times should return an integer when divided by number of seconds in a day.
    # step size check.

    d = (utils.today() - parse("20200101T000000 UTC").timestamp()) / SECONDS_IN_A_DAY
    logger.info(f"Number of seconds between any time points must be a multiple of number of seconds in a day. Found "
                f"a multiple which is equals to {d}.")
    assert d == round(d)


def test_ticker_column_must_be_categorical():
    # dtype check.
    # Avoid object dtypes as they are slow.
    df = parse_file(filename='../examples/portfolio_02.csv')
    assert isinstance(df['ticker'].dtype, pd.api.types.CategoricalDtype)


def test_action_column_must_be_categorical():
    # dtype check for the parsed transaction table.
    # Avoid object dtypes as they are slow.
    df = parse_file(filename='../examples/portfolio_02.csv')
    assert pd.api.types.is_categorical_dtype(df.action)
    assert pd.api.types.is_categorical_dtype(df.ticker)


def test_position_data_types():
    pos = Position('buy', 4, 'AAPL', 377481600)
    assert pd.api.types.is_categorical_dtype(pos.df.action)
    assert pd.api.types.is_categorical_dtype(pos.df.ticker)
    assert pd.api.types.is_float_dtype(pos.df.quantity) or pd.api.types.is_int64_dtype(pos.df.quantity)
    assert pd.api.types.is_int64_dtype(pos.df.date)
    assert pd.api.types.is_float_dtype(pos.df.price)


def test_is_index_sorted_in_time_course_table():
    df = parse_file(filename='../examples/portfolio_01.csv')
    p = Portfolio(df)
    assert p.table_time_course_asset_price.index.is_monotonic


def test_no_nan_rows_in_time_course_tables():
    # There must never be any single row which is full of nans. At least one column must have a non-nan value.
    df = parse_file(filename='../examples/portfolio_01.csv')
    p = Portfolio(df)
    logger.info(f"There should be no row with only nan values.")
    assert p.table_time_course_asset_price.isna().sum(axis=1).max() < p.table_time_course_asset_price.shape[1]


def test_lot_numbers():
    # When the same ticker is entered N times, the max available lot number should be equal to N.
    df = parse_file(filename='../examples/portfolio_03.csv')
    p = Portfolio(df)
    a = p.table_transaction.groupby('ticker').count()['quantity'].to_frame()
    b = p.table_transaction.groupby('ticker').max()['lot'].to_frame()
    df = pd.DataFrame.join(a, b)
    assert df['quantity'].equals(df['lot'])


def test_asset_price():
    # As the database is currently generating random numbers, this will fail.
    logger.info(f"If we randomly select an entry from the asset price time-series, it should match the value in the "
                f"database.")
    df = parse_file(filename='../examples/portfolio_03.csv')
    p = Portfolio(df)
    out = p.table_time_course_asset_price.unstack(-1)
    out = out.loc[out.isna() == False].sample(1).reset_index()
    s = db.read(out['level_0'].values[0], out['date'].to_list(), output_format='series')
    assert s.iloc[0] == out[0].values[0]


def test_time_course_tables():
    #  simple test to check if time-course tables run at all.
    df = parse_file(filename='../examples/portfolio_03.csv')
    p = Portfolio(df)
    assert type(p.table_time_course_asset_price) == pd.DataFrame
    assert type(p.table_time_course_asset_cost) == pd.DataFrame
    assert type(p.table_time_course_asset_returns) == pd.DataFrame
    assert type(p.table_time_course_asset_quantity) == pd.DataFrame


def test_return_at_purchase_date():
    # return at purchase date must be 100%
    df = parse_file(filename='../examples/portfolio_01.csv')
    p = Portfolio(df)
    for i in p.table_transaction[["ticker", "date"]].to_dict(orient='records'):
        print(i)
        df = p.table_time_course_asset_returns
        assert df.loc[i['date'], i['ticker']] == 100.


def test_check_quantity_at_date_of_purchase():
    df = parse_file(filename='../examples/portfolio_03.csv')
    p = Portfolio(df)
    df = p.table_time_course_asset_quantity
    for ticker in p.table_transaction["ticker"].unique():
        # transaction dates
        rows = p.table_transaction['ticker'] == ticker
        transaction_dates = p.table_transaction.loc[rows, 'date']
        q_time_course = df.loc[transaction_dates, ticker]

        q_transaction_table = p.table_transaction.loc[rows, 'quantity'].cumsum()
        assert np.all(q_transaction_table.values == q_time_course.values)
# def test_check_quantity_at_today():
#     # The amount of stocks owned must be +s of buys and -s of sells
#     df = parse_file(filename='../examples/portfolio_03.csv')
#     p = Portfolio(df)
#     1

#  TODO: asset return at purchase date must be 100%
#  TODO: Transaction table must have indices from 0 to the shape[0] of the table.
#  TODO: Two times the same ticker must result in two different columns in the time-course table.
#  TODO: When same ticker added twice the asset price must be the mean of them
