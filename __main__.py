import typer
from portfolio.Parser import PortfolioParser
from portfolio.Plotter import console_plot
from portfolio.Portfolio import Portfolio

from downloader.downloader import updater

app = typer.Typer(add_completion=False)


@app.command()
def db_update(ticker=None):
    """
    Update all tickers in the DB for all tickers. Alternatively can also update only the passed tickers.
    Args:
        ticker: (list) Valid Nasdaq tickers.
    """
    updater(ticker)


@app.command()
def parse_export(filename: str) -> str:
    """
    Parse a personal portfolio .csv file (e.g. export from Yahoo Finance) to a transaction table.
    Returns:
        Transaction table as JSON.
    """
    df = PortfolioParser(filename).df
    out = df.to_json(orient='records')
    print(out)
    return out


@app.command()
def get_time_course(filename: str):
    """
    Returns time-course of asset prices in the portfolio
    Args:
        filename:
    Returns:
        df: with as many columns as tickers in portfolio indexed on time.
    """
    pp = PortfolioParser(filename)
    p = Portfolio(pp.tickers)
    console_plot(p.returns, )


#  TODO: plot ticker


if __name__ == "__main__":
    app()
