import click
import duckdb
from preprocess import add_data
from CONSTANTS import DEFAULTDATABASE
from pathlib import Path
from parse_links import url_parser
import os

@click.command()
@click.argument('data')
@click.option('--database', required=False)
def main(data, database):

    # Set up a database in which to store everything
    if not database:
        database = DEFAULTDATABASE
    db_path = Path(database)
    if not db_path.parent.exists():
        os.mkdir(db_path.parent)

    # Connect to the database
    connection = duckdb.connect(database=database, read_only=False)

    # Preprocess data and insert to database
    add_data(data, connection)

    # Parse links
    url_parser(connection)


if __name__ == "__main__":
    main()
