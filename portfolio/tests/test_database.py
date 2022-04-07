import pandas as pd
from portfolio.Database import DB
import time
import numpy as np

db = DB()
# a new index name
test_index = "pytest" + str(int(time.time()))


def test_create_index():
    assert db.setup_es_index(test_index)


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


def test_write_one_line():
    """
    Writes a new document to ES and reads it. Both dataframes must be equal.
    """
    time_point = 11111
    ticker = 'AAPL'
    value = 12.0
    # create local data
    s = pd.Series(value, name=ticker, index=pd.Index(data=[time_point], name='date'))
    df = db.convert(s)
    time.sleep(2)
    # write it to database
    db.write(index_name=test_index, df=df)
    time.sleep(2)
    # read it back
    df_new = db.read(ticker, index_name=test_index, date=[time_point])
    assert df.equals(df_new)


def test_read_one_line_dataframe():
    df = db.read(ticker='AAPL', date=[1648598400])
    assert df.shape[0] == 1
    assert type(df) == pd.DataFrame


def test_read_one_line_series():
    df = db.read(ticker='AAPL', date=[1648598400], output_format='series')
    print(df.head())
    assert type(df) is pd.Series


def test_returned_shape():
    size_ = np.ceil(np.random.random(size=1) * 100)+1
    time_point = np.ceil(np.random.random(size=int(size_[0])) * 100000)+1
    value = np.random.random(size=int(size_[0])) * 100000
    ticker = 'AAPL__'
    s = pd.Series(value, name=ticker, index=pd.Index(data=time_point, name='date'))
    df_new = db.convert(s)
    to_be_added = df_new.shape[0]
    print(f"to be added: {to_be_added}")
    # I think the db calls are not blocking, hence I need sleep statements otherwise the added table do not come back
    # when I read.
    df_old = db.read(ticker=ticker, output_format='raw', index_name=test_index)
    time.sleep(5)
    old_size = df_old.shape[0]
    print(f"previous size: {old_size}")
    db.write(index_name=test_index, df=df_new)
    time.sleep(5)
    df_final = db.read(ticker=ticker, output_format='raw', index_name=test_index)
    final_size = df_final.shape[0]
    print(f"new size: {final_size}, should be {old_size+to_be_added}")
    time.sleep(5)
    assert final_size == (old_size + to_be_added)


def test_equality_of_time_points():
    indices = [1599696000, 1599782400, 1600041600, 1600128000]
    df = db.read(ticker='AAPL', date=indices, output_format='series')
    assert np.all(df.index == indices)


def test_no_day_should_return_empty():
    # we just add 1 to above time indices to break the match in the database.
    indices = [1599696001, 1599782401, 1600041601, 1600128001]
    df = db.read(ticker='AAPL', date=indices)
    assert df.empty