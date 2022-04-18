from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from portfolio.Ticker import Ticker
from typing import Dict, List
import pandas as pd
import numpy as np


def test_transaction_table_data_types():
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    df = pp.df

    assert pd.api.types.is_categorical_dtype(df.action)
    assert pd.api.types.is_categorical_dtype(df.ticker)
    assert pd.api.types.is_float_dtype(df.quantity) or pd.api.types.is_int64_dtype(df.quantity)
    assert pd.api.types.is_int64_dtype(df.date)
    assert pd.api.types.is_int64_dtype(df.date_human)
    assert pd.api.types.is_float_dtype(df.current_price)


def test_position_list_output():
    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    assert type(pp.positions) == list


def test_position_dict_output():
    # simply tests if the output of grouped_positions is a dict and that only one ticker is present in the grouped
    # positions.
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    out = pp.grouped_positions
    assert type(out) == dict
    example_ticker = list(out.keys())[0]
    assert len(np.unique([pos.ticker for pos in out[example_ticker]])) == 1


def test_position_presence_of_sell_action():
    # when using an export file with sell actions in it, do we get them parsed correctly?
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert set(pp.df['action'].values) == set(('buy', 'sell'))


def test_position_df_output():
    # simply tests if the output of grouped_positions is a dict and that only one ticker is present in the grouped
    # positions.
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    example_ticker = list(pp.grouped_positions_df.keys())[0]
    df = pp.grouped_positions_df[example_ticker]
    assert type(df) == pd.DataFrame
    assert len(np.unique(df.ticker)) == 1


def test_sold_negative():
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert np.all(pp.df.loc[pp.df.action == 'sell', 'quantity'] < 0)
    assert np.all(pp.df.loc[pp.df.action == 'buy', 'quantity'] > 0)


def test_parsed_tickers():
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert type(pp.ticker_names) == list


def test_grouped_Ticker_output():
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    t = pp.ticker_names[0]
    assert type(pp.grouped_tickers[t]) == Ticker


def test_Ticker_output():
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert type(pp.tickers) == list
    assert type(pp.tickers[0]) == Ticker
    assert pp.df['ticker'].nunique() == len(pp.tickers)

    filename = '../examples/portfolio_01.csv'
    pp = PortfolioParser(filename)
    assert type(pp.tickers) == list
    assert type(pp.tickers[0]) == Ticker
    assert pp.df['ticker'].nunique() == len(pp.tickers)
