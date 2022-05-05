import pandas as pd
from portfolio.Database import DB
import time
import numpy as np
from portfolio import utils

db = DB()
# a new index name
test_index = "pytest" + str(int(time.time()))


def test_converter():
    """
    Calling two time the converter should return the original object.
    """
    time_point = 11111
    ticker = 'AAPL'
    value = 12.0
    s = pd.Series({time_point: value}, name=ticker)
    assert db.convert(db.convert(s)).equals(s)


def test_converter_required_columns():
    """
    when a pd.Series is converted to df, check if the df has the required columns and index names in place for it to be
    converted back to a pd.Series.
    """
    time_point = 11111
    ticker = 'AAPL'
    value = 12.0
    s = pd.Series(value, name=ticker, index=pd.Index(data=[time_point], name='date'))
    df = db.convert(s)
    assert set(df.reset_index().columns) == {'date', 'Close', 'ticker'}


def test_conversion_of_empty_dataframe():
    # We choose on purpose an impossible date, which will return an empty df,
    # converting that df to series should return an empty series.
    df = db.read(ticker='AAPL', date=[0])
    assert type(db.convert(df)) == pd.Series


def test_read_one_line_dataframe():
    a_monday = 1649635200
    df = db.read(ticker='AAPL', date=[a_monday])
    assert df.shape[0] == 1
    assert type(df) == pd.DataFrame


def test_read_one_line_series():
    df = db.read(ticker='AAPL', date=[1648598400], output_format='series')
    print(df.head())
    assert type(df) is pd.Series


def test_equality_of_time_points():
    # send a vector of dates to db, and compare this to the index of the returned dataframe/series.
    start_time = 1599696000
    indices = np.arange(start_time, utils.today(), (60 * 60 * 24), dtype=int)
    df = db.read(ticker='MSFT', date=indices, output_format='series')
    #  TODO: should the database return not found time points with Nans, or should only return the ones that are found?
    # Until this question is answered this test will fail, the next one is a related question.
    # also there lots of days where the data should be missing, such as holidays etc.
    assert np.all(df.index == indices)
