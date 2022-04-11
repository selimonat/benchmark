import pandas as pd
from uniplot import plot

# TODO: make it possible to plot to string and use it as a debugger.


def console_plot(s: pd.Series, remove_nans=False):
    """
    Simple console plotter
    Args:
        s: Pandas series with a time index
        remove_nans: (bool) Discards NaNs befores plotting (default). If False, zeroes all nans.

    Returns:
        None. Prints a plot to the console.
    """

    valid_index = ~s.T.isna()
    if remove_nans:
        x = s.index.values[valid_index] / (60*60*24*365) + 1970
        y = s.T.values[valid_index]
    else:
        s.T.values[~valid_index] = 0
        x = s.index.values / (60*60*24*365) + 1970
        y = s.T.values

    plot(xs=x,
         ys=y,
         lines=True,
         x_unit='date',
         width=150)
