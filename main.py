import typer
from portfolio import Parser as Parser
from portfolio.Portfolio import Portfolio
app = typer.Typer(add_completion=False)


@app.command()
def parse_export(filename: str):
    """
    Parse a personal portfolio .csv file (e.g. export from Yahoo Finance) to a transaction table.
    Currently mock state.

    Returns:
        Transaction table.
    """
    df = Parser.parse_file(filename)
    print(df)


@app.command()
def get_transactions(filename: str):
    """
    Convert a portfolio export to a transaction table.
    Args:
        filename:
    Returns:
        df: with 5 columns ticker, date, price, quantity, action
    """
    df = Parser.parse_file(filename)
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


if __name__ == "__main__":
    app()
