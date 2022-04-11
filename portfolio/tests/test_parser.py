from portfolio.Parser import PortfolioParser
from portfolio.Position import Position
from typing import Dict
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
    assert type(pp.positions) == Position


def test_position_dict_output():
    # simply tests if the output of grouped_positions is a dict and that only one ticker is present in the grouped
    # positions.
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert type(pp.grouped_positions) == dict
    assert len(np.unique([pos.ticker for pos in pp.grouped_positions['FB']])) == 1


def test_position_presence_of_sell_action():
    # when using an export file with sell actions in it, do we get them parsed correctly?
    filename = '../examples/portfolio_05.csv'
    pp = PortfolioParser(filename)
    assert set(pp.df['action'].values) == set(('buy','sell'))
