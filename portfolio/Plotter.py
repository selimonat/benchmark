import pandas as pd
from uniplot import plot
from typing import Union


def console_plot(s: Union[pd.Series, pd.DataFrame], remove_nans=False):
    """
    Simple console plotter
    Args:
        s: Pandas Series or DataFrame  with a time index.
        remove_nans: (bool) Discards NaNs before plotting (default). If False, zeroes all nans.

    Returns:
        None. Prints a plot to the console.
    """
    if isinstance(s, pd.Series):
        s = s.to_frame()
    valid_index = ~s.isna().any(axis=1)
    if remove_nans:
        x = s.index[valid_index].values / (60 * 60 * 24 * 365) + 1970
        y = s[valid_index].T.values
    else:
        s.loc[~valid_index] = 0
        x = s.index.values / (60 * 60 * 24 * 365) + 1970
        y = s.T.values

    plot(xs=[x for _ in range(len(y))],  # repeat the x as many time as to match len(y)
         ys=y,
         lines=True,
         x_unit='date',
         width=200)
