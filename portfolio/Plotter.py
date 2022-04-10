import pandas as pd


def plot(s: pd.Series):
    from uniplot import plot
    valid_index = ~s.T.isna()
    x = s.index.values[valid_index] / (60*60*24*365) + 1970
    y = s.T.values[valid_index]
    plot(xs=x,
         ys=y,
         lines=True,
         x_unit='date',
         width=150)
