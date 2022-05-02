"""Module to parse portfolios exported to csv file"""
import pandas as pd
import portfolio.utils as utils
from dateutil import parser
from typing import AnyStr, List, Dict
from portfolio.Position import Position
from portfolio.Ticker import Ticker
from collections import defaultdict
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# used to map all column names listed in values, to their keys {target column name: export file column name}.
# the column names listed as values are encountered on the .csv file and then mapped to final column names (shown in
# keys).
mapping = {'action': ['action', 'type'],
           'quantity': ['quantity', 'amount', 'n'],
           'ticker': ['symbol', 'ticker'],
           'date_human': ['trade date', 'purchase date']
           }

actions = {'buy': ['bought', 'buy'], 'sell': ['sold', 'sell']}


class PortfolioParser:

    def __init__(self, filename: AnyStr):
        """
        Parses a portfolio export file where each row represents an opened/closed transaction. Attempts to understand
        action, quantity, ticker name and date variables.

        It features the following properties:

        self.df: The parsed file with each row being a transaction, these rows are not validated, meaning no errors
        are thrown when something is wrong with the values.

        self.positions: Returns each row as a validated Position object in a list. For validation see the Position
        class.

        self.{*positions, *tickers} are all build upn the self.positions method and represents some handy grouping of
        the positions. For example, self.grouped_positions returns a dict as {ticker: [positions]}.

        Args:
            filename: (str)
        """
        self.logger = utils.get_logger(__name__)
        self.filename = filename
        self.df = self.parse_file(self.filename)

    @property
    def ticker_names(self):
        # list of tickers present in this parsum
        return list(self.grouped_positions.keys())

    @property
    def grouped_tickers(self) -> Dict[AnyStr, Ticker]:
        """
        Grouped Ticker objects for each position.
        """
        d = defaultdict(list)
        for ticker_name, positions in self.grouped_positions.items():
            d[ticker_name] = Ticker(positions)
        return dict(d)

    @property
    def tickers(self) -> List[Ticker]:
        """
        A list of Ticker objects, to be fed directly to Portfolio.
        """
        d = list()

        for ticker_name, positions in self.grouped_positions.items():
            d.append(Ticker(positions))
        return d

    @property
    def grouped_positions_df(self) -> Dict[AnyStr, pd.DataFrame]:
        """
        Returns: Returns parsed .csv export file as a dict organized as {ticker:df]}.
        """
        # convert values to DataFrames
        d = defaultdict(list)
        for ticker, positions in self.grouped_positions.items():
            d[ticker] = pd.concat([pos.df for pos in positions])
        return dict(d)

    @property
    def grouped_positions(self) -> Dict[AnyStr, List[Position]]:
        """
        Returns: Returns parsed .csv export file as a dict organized as {ticker:[positions]}.
        """
        d = defaultdict(list)
        for pos in self.positions:
            d[pos.ticker].append(pos)

        return dict(d)

    @property
    def positions(self) -> List:
        """
        Returns: Returns parsed .csv export file as a list of validated Position objects.
        """

        def callback(col) -> Position:
            return Position(col.action, col.quantity, col.ticker, int(col.date))

        return self.df.T.apply(callback).to_list()

    def parse_file(self, filename: AnyStr) -> pd.DataFrame:
        """
        Extracts 5 columns from the export: current_price, action, quantity, ticker and date. If action not present, assumes buy.
        Args:
            filename:

        Returns:
            df: parsed data structure as pandas dataframe with the following structure:
                df.index (categorical):
                df.action (categorical):
                df.quantity (float):
                df.ticker (categorical):
        """
        df = pd.read_csv(filename)
        # rename columns
        df = df.rename(mapper=self.column_mapper, axis=1)
        # clean columns
        df = df.loc[:, df.columns[df.columns.isna() == False]]
        # are all required columns present?
        if len(set(mapping)) != len(set(df.columns)):
            self.logger.info('Not all wanted columns there.')
            missing = set(mapping) - set(df.columns)
            self.logger.info(f'Missing is {missing}')
            # Column completion:
            if 'action' in missing:
                self.logger.info(f'Assuming missing actions as buy.')
                df['action'] = 'buy'

            if (set(mapping) - set(df.columns)) != set():
                self.logger.info("No more missing columns")
                # TODO: Test if the exception is correctly raised.
                raise Exception(f'Cannot continue with missing incomplete columns list:\n {df.columns}')
        self.logger.debug('lowering all action names.')
        df['action'] = df['action'].str.lower()
        self.logger.debug('Parsed export file as follows\n' + df.to_string())
        self.logger.info('Parsing date column.')
        df['date'] = df['date_human'].astype(str).apply(parser.parse).astype(int) / 10 ** 9
        df['date'] = df['date'].astype(int)
        self.logger.info('Sorting transaction table by date')
        df.sort_values(by='date', inplace=True)
        self.logger.info('Adjusting datatypes, avoiding objects.')
        df['action'] = df['action'].astype("category")
        df['ticker'] = df['ticker'].astype("category")
        # make quantities negative for sold shares
        i = df.action == 'sell'
        df.loc[i, 'quantity'] = df.loc[i, 'quantity'] * -1

        # Uniformize actions.
        def uniformized_action_row(name):
            for k, v in actions.items():
                if name in v:
                    return k
        df['action'] = df['action'].map(uniformized_action_row)
        if df['action'].isna().any():
            raise Exception('There are nans in the action column.')

        # Validity check: if a complete row is na stop.
        if (df.isna().sum(axis=1) == df.shape[1]).any():
            raise Exception(f'Found one complete nan row in the parsed portfolio export file.')
        self.logger.debug('Parsed, now it looks like this\n' + df.to_string())
        return df

    def column_mapper(self, old_colname):
        """
        Maps column names as specified in the mapping variable. Standardize column names by mapping similar ones to the
        same one.
        It is called by the rename method of pandas.
        Args:
            old_colname:
        Returns:
            new column name, which are
        """

        new_colname = [k for k, v in mapping.items() if old_colname.lower() in v]
        self.logger.debug(f'Mapping {old_colname} to {new_colname}')

        if not new_colname:
            return None
        else:
            return new_colname[0]
