import pandas as pd
from portfolio.Database import DB
import time
import numpy as np

db = DB()
# a new index name
test_index = "pytest" + str(int(time.time()))


def create_df(date, ticker):
    df = pd.DataFrame(data=[], columns=['ticker', 'Close'], index=pd.Index(date, name='date'))
    df.ticker = ticker
    df.Close = np.random.rand(len(date))
    return df


def test_create_index():
    assert db.setup_es_index(test_index)


def test_returned_shape():
    db.setup_es_index(test_index)
    size_ = np.ceil(np.random.random(size=1) * 100) + 1
    time_point = np.ceil(np.random.random(size=int(size_[0])) * 100000) + 1
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
    print(f"new size: {final_size}, should be {old_size + to_be_added}")
    time.sleep(5)
    assert final_size == (old_size + to_be_added)


def test_weekend():
    # Asking for a weekend we should get a nan, and when requested the nan must be filled in with the previous date.
    a_friday = 1637971200 - 24 * 60 * 60
    a_saturday = 1637971200
    df = db.read(ticker='FB', date=[a_friday, a_saturday], fill_na=False)
    assert df['Close'].isna().sum() == 1
    df = db.read(ticker='FB', date=[a_friday, a_saturday], fill_na=True)
    assert df['Close'].isna().sum() == 0
    assert df['Close'].iloc[0] == df['Close'].iloc[1]  # because of forward filling
    # test now with a unlikely time value, i.e. 0
    df = db.read(ticker='FB', date=[0], fill_na=False)
    assert df.index.isin([0])


def test_write_one_line():
    """
    Writes a new document to ES and reads it. Both dataframes must be equal.
    """
    db.setup_es_index(test_index)
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


def test_write_after_deleting():
    # create a fake df, send it to db, and read it back, are they same?
    db.setup_es_index(test_index)
    ticker = 'blablablalbalblalbla'
    date = [0, 1, 2]
    df0 = create_df(date, ticker)
    db.write(test_index, df0)
    time.sleep(5)
    df0r = db.read(ticker=ticker, index_name=test_index, date=date)
    # does the size match?
    assert df0r.shape[0] == 3
    df_ = db.read(ticker=ticker, index_name=test_index, date=[1])
    # does the content match?
    assert df_.shape[0] == 1
    # do we get nan when asked for non-existing data?
    df_ = db.read(ticker=ticker, index_name=test_index, date=[3])
    assert df_['Close'].isna().all()

    # now add more data under the same ticker name. The data here is overlapping with the previous data, but ES has
    # no knowledge of duplication so it will just duplicate them (because the doc_ids will be different).
    date_2 = [0, 1, 2, 3]
    df2 = create_df(date_2, ticker)
    print(f"Now this one:\n{df2}")
    db.write(test_index, df2)
    time.sleep(5)
    df2r = db.read(ticker=ticker, index_name=test_index)
    # do we have the both dataset together ?
    assert df2r.shape[0] == (len(date) + len(date_2))

    # now delete the whole data and re-write it
    db.delete_ticker(ticker, test_index)
    db.write(test_index, df2)
    time.sleep(5)
    df3r = db.read(ticker=ticker, index_name=test_index)
    # do we only get the new freshly written data?
    assert df3r.shape[0] == len(date_2)


def test_duplicate_entries_should_not_increase_number_of_documents():
    db.setup_es_index(test_index)
    date = [0]
    ticker = 'blabla'
    df = create_df(date, ticker)
    db.write(test_index, df)
    time.sleep(5)
    df_ = db.read(ticker=ticker, index_name=test_index, date=date)
    assert df.shape[0] == df_.shape[0]
    assert df_.shape[0] == 1
    # write it again and the size shouldn't change
    db.write(test_index, df)
    time.sleep(5)
    df_ = db.read(ticker=ticker, index_name=test_index, date=date)
    assert df.shape[0] == df_.shape[0]
    assert df_.shape[0] == 1
