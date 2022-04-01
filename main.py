import typer

app = typer.Typer(add_completion=False)


@app.command()
def parse_to_transactions(filename: str):
    """
    Parse a personal portfolio .csv file (e.g. export from Yahoo Finance) to a transaction table.
    Currently mock state.
    Args:
        filename: path to the file.

    Returns:
        Transaction table.
    """
    typer.echo(f"Hello {filename}")


if __name__ == "__main__":
    app()
