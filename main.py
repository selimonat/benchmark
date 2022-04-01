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
    df = Parser.parse(filename)
    print(df)
    return df

@app.command()
def get_transactions(filename: str):
    """
    Convert a portfolio export to a transaction table.
    Args:
        filename:
    Returns:
        df: with 5 columns ticker, date, price, quantity, action
    """
    df = Parser.parse(filename)
    p = Portfolio(df)
    print(p.transactions)
    return p


if __name__ == "__main__":
    app()
