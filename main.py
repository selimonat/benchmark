import typer
from portfolio import Parser
app = typer.Typer(add_completion=False)


@app.command()
def parse_to_transactions(filename: str):
    """
    Parse a personal portfolio .csv file (e.g. export from Yahoo Finance) to a transaction table.
    Currently mock state.

    Returns:
        Transaction table.
    """
    df = Parser.parse(filename)

    return df


if __name__ == "__main__":
    app()
