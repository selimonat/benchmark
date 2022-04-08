from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
import pandas as pd


def test_transaction_table_data_types():
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    df = pp.df

    assert pd.api.types.is_categorical_dtype(df.action)
    assert pd.api.types.is_categorical_dtype(df.ticker)
    assert pd.api.types.is_float_dtype(df.quantity) or pd.api.types.is_int64_dtype(df.quantity)
    assert pd.api.types.is_int64_dtype(df.date)
    assert pd.api.types.is_int64_dtype(df.date_human)
    assert pd.api.types.is_float_dtype(df.price)


def test_position_output():
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    assert type(pp.positions) == Position
