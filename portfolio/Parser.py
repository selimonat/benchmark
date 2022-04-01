"""Module to parse portfolios exported to csv file"""
import pandas as pd
import portfolio.utils as utils

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = utils.get_logger(__name__)

mapping = {'price': ['purchase price'],
           'action': [],
           'quantity': ['quantity', 'amount'],
           'ticker': ['symbol'],
           'date': ['trade date', 'purchase date']
           }


def parse(filename):
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
    return df


def column_mapper(old_colname):

    new_colname = [k for k, v in mapping.items() if old_colname.lower() in v]
    logger.debug(f'Mapping {old_colname} to {new_colname}')

    if not new_colname:
        return None
    else:
        return new_colname[0]


if __name__ == '__main__':
    df2 = parse(filename='examples/portfolio_01.csv')
