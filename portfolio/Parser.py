"""Module to parse portfolios exported to csv file"""
import pandas as pd
import portfolio.utils as utils
from dateutil import parser

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = utils.get_logger(__name__)

# used to map all columns names listed in values, to their keys.
mapping = {'price': ['purchase price'],
           'action': [],
           'quantity': ['quantity', 'amount'],
           'ticker': ['symbol'],
           'date_human': ['trade date', 'purchase date']
           }


def parse_file(filename):
    """
    Extracts 5 columns from the export: price, action, quantity, ticker and date. If action not present, assumes buy.
    Args:
        filename:

    Returns:
        df: parsed data structure as pandas dataframe.
    """
    df = pd.read_csv(filename)
    # rename columns
    df = df.rename(mapper=column_mapper, axis=1)
    # clean columns
    df = df.loc[:, df.columns[df.columns.isna() == False]]
    # are all required columns present?
    if len(set(mapping)) != len(set(df.columns)):
        logger.info('Not all wanted columns there.')
        missing = set(mapping) - set(df.columns)
        logger.info(f'Missing is {missing}')
        if missing == set(('action',)):
            logger.info(f'Assuming missing actions as buy.')
            df['action'] = 'buy'
    logger.debug('Parsed export file as follows\n' + df.to_string())
    logger.info('Parsing date column.')
    df['date'] = df['date_human'].astype(str).apply(parser.parse).astype(int) / 10 ** 9
    logger.info('Sorting transaction table by date')
    df.sort_values(by='date', inplace=True)
    logger.debug('Converted, now it looks like this\n' + df.to_string())
    logger.info('Adjusting datatypes, avoiding objects.')
    df['action'] = df['action'].astype("category")
    df['ticker'] = df['ticker'].astype("category")
    return df


def column_mapper(old_colname):
    """
    Maps column names as specified in the mapping variable. Standardize column names by mapping similar ones to the
    same one e.g. Purchase Price -> price; buy Price -> price, etc.
    It is called by the rename method of pandas.
    Args:
        old_colname:
    Returns:
        new column name, which are
    """

    new_colname = [k for k, v in mapping.items() if old_colname.lower() in v]
    logger.debug(f'Mapping {old_colname} to {new_colname}')

    if not new_colname:
        return None
    else:
        return new_colname[0]


if __name__ == '__main__':
    df2 = parse_file(filename='examples/portfolio_02.csv')
    from portfolio.Portfolio import Portfolio
    p = Portfolio(df2)

