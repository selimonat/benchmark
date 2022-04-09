import typer
from portfolio.Parser import PortfolioParser
from portfolio.Portfolio import Portfolio
app = typer.Typer(add_completion=False)


def parse_export(filename: str):
    """
    Parse a personal portfolio .csv file (e.g. export from Yahoo Finance) to a transaction table.
    Currently mock state.

    Returns:
        Transaction table.
    """
    df = PortfolioParser(filename).df
    print(df)


@app.command()
def get_transactions(filename: str):
    """
    Convert a portfolio export to a transaction table.
    Args:
        filename:
    Returns:
        df: with 5 columns ticker, date, current_price, quantity, action
    """
    df = PortfolioParser(filename).df
    p = Portfolio(df)
    print(p.table_transaction)


@app.command()
def get_time_course(filename: str):
    """
    Returns time-course of asset prices in the portfolio
    Args:
        filename:
    Returns:
        df: with as many columns as tickers in portfolio indexed on time.
    """
    df = Parser.parse_file(filename)
    p = Portfolio(df)
    print(p.table_time_course_asset_price.tail(10))

#  TODO: plot ticker


if __name__ == "__main__":
    app()
